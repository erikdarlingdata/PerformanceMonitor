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
/// Cross-SKU text guard for #3648: the <c>top_cpu_queries</c> and <c>bad_actor_query</c> drill-down SQL must
/// be BYTE-IDENTICAL between Lite's inline <c>cmd.CommandText</c> and Darling's named constants, and both
/// must carry the per-plan provenance shape rather than the old hash-folded <c>MAX(max_dop)</c>.
///
/// <para><b>Why identical text and not two independent pins.</b> #2705 fixed Darling's stale-max_dop
/// cross-check and closed without a record that Lite had a twin; #2999 is what that cost. The two
/// drill-down reads here were already character-for-character the same before #3648 (the port kept Lite's
/// text), and the provenance columns are engine-neutral (<c>ROW_NUMBER() OVER</c>, <c>MAX() OVER</c>,
/// <c>FILTER</c>-free <c>CASE</c> aggregates, explicit <c>NULLS LAST</c>), so the strongest guard available
/// is equality: a future edit to one SKU's read fails here until the other is brought along. Lite's SQL is
/// inline, Darling's is a constant, and no test project references both assemblies, so both sides are read
/// from the checked-out tree through <see cref="ParitySource"/>, as the other parity pairs do.</para>
/// </summary>
public sealed class DrillDownDopProvenanceParityTests
{
    private const string LiteFile = "Lite/Analysis/DrillDownCollector.Queries.cs";
    private const string DarlingFile = "Darling/PerformanceMonitor.Darling.Analysis/PgDrillDownCollector.Queries.cs";

    [Theory]
    [InlineData("CollectTopCpuQueries", "TopCpuQueriesSql")]
    [InlineData("CollectBadActorDetail", "BadActorDetailSql")]
    public void TheDrillDownSql_IsByteIdenticalAcrossSkus(string liteMethod, string darlingConst)
    {
        var lite = LiteInlineSql(liteMethod);
        var darling = DarlingConstSql(darlingConst);

        Assert.Equal(darling, lite);
    }

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
    public void NeitherSku_CoercesAnUnknownDopToZero()
    {
        /* The DMV never reports 0 — a serial plan is 1 — so `IsDBNull ? 0` on a DOP ordinal was "no reading"
           rendered as a degree of parallelism. Both readers must project null. Scoped to the two files'
           max_dop reads: the other drill-downs' zero-coercions (counts, sums) are legitimately 0-when-absent. */
        foreach (var file in new[] { LiteFile, DarlingFile })
        {
            var source = ParitySource.ReadFile(file);
            Assert.DoesNotMatch(new Regex(@"max_dop\s*=\s*reader\.IsDBNull\(\d+\)\s*\?\s*0\b"), source);
            Assert.Matches(new Regex(@"var maxDop = reader\.IsDBNull\(4\) \? \(int\?\)null"), source);
            Assert.Matches(new Regex(@"var maxDop = reader\.IsDBNull\(10\) \? \(int\?\)null"), source);
            Assert.Contains("dop_note = QueryDopProvenance.Note(maxDop, maxDopAnyPlan, maxDopAnyPlanLastSeen, planCount)", source, StringComparison.Ordinal);
        }
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

    /// <summary>Darling's named constant: <c>public const string {name} = @"..."</c>, same literal rules.</summary>
    private static string DarlingConstSql(string constName)
    {
        var source = ParitySource.ReadFile(DarlingFile);
        var marker = $"public const string {constName} = @\"";
        var start = source.IndexOf(marker, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{constName} not found in {DarlingFile}");
        var literalStart = start + marker.Length;
        var literalEnd = source.IndexOf("\";", literalStart, StringComparison.Ordinal);
        return source[literalStart..literalEnd];
    }
}
