/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8470: the <c>top_cpu_queries</c> and <c>bad_actor_query</c> drill-down reads are written once, as
/// <c>PerformanceMonitor.Common.DrillDownSql.TopCpuQueries</c> and <c>DrillDownSql.BadActorDetail</c>, and both
/// collectors run that text. Darling's <see cref="PgDrillDownCollector.TopCpuQueriesSql"/> and
/// <see cref="PgDrillDownCollector.BadActorDetailSql"/> are that constant under the names the drill-down tests
/// use; Lite's <c>CollectTopCpuQueries</c> and <c>CollectBadActorDetail</c> assign it to their command, which its
/// half of <c>DrillDownDopProvenanceParityTests</c> resolves. The rules the text carries, the #3648 per-plan
/// <c>max_dop</c> provenance and the #8317 <c>$5</c> database filter, are pinned here on the one text.
///
/// <para>The constant is reached by name, so this class compiles against a Common that lacks it and fails at its
/// first assert naming what is missing.</para>
/// </summary>
public sealed class DrillDownSqlTests
{
    private static string CommonText(string name)
    {
        var type = typeof(QueryDopProvenance).Assembly.GetType("PerformanceMonitor.Common.DrillDownSql");
        Assert.True(type is not null, "PerformanceMonitor.Common has no DrillDownSql: the two drill-down reads are not written once in Common.");
        var field = type!.GetField(name, BindingFlags.Public | BindingFlags.Static);
        Assert.True(field is { IsLiteral: true } && field.FieldType == typeof(string), $"DrillDownSql.{name} is not a public const string.");
        return (string)field!.GetRawConstantValue()!;
    }

    [Fact]
    public void Darling_RunsTheCommonText_UnderItsOwnNames()
    {
        Assert.Equal(PgDrillDownCollector.TopCpuQueriesSql, CommonText("TopCpuQueries"));
        Assert.Equal(PgDrillDownCollector.BadActorDetailSql, CommonText("BadActorDetail"));
    }

    [Theory]
    [InlineData("TopCpuQueries")]
    [InlineData("BadActorDetail")]
    public void TheCommonText_CarriesThePerPlanProvenanceShape_NotTheHashFoldedMaximum(string name)
    {
        var sql = CommonText(name);

        /* #3648: the headline is the NEWEST plan's reading, picked by a newest-first ranking with the tie-breakers
           spelled out (compile time, then CPU spent) and their null placement made explicit, because DuckDB and
           Postgres default DESC null placement differently. */
        Assert.Matches(
            new Regex(@"ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION BY database_name, query_hash\s+ORDER BY collection_time DESC, creation_time DESC NULLS LAST, delta_worker_time DESC NULLS LAST\s*\)\s+AS newest_rn", RegexOptions.Singleline),
            sql);
        Assert.Contains("MAX(CASE WHEN newest_rn = 1 THEN max_dop END) AS max_dop", sql, StringComparison.Ordinal);

        /* The history: how many plans the group spans, the cross-plan maximum, and when it was last seen. */
        Assert.Contains("COUNT(DISTINCT query_plan_hash) AS plan_count", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(max_dop) OVER (PARTITION BY database_name, query_hash) AS max_dop_any_plan", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(max_dop_any_plan) AS max_dop_any_plan", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(CASE WHEN max_dop = max_dop_any_plan THEN collection_time END) AS max_dop_any_plan_last_seen", sql, StringComparison.Ordinal);

        /* No bare hash-folded MAX(max_dop) projected as max_dop. */
        Assert.DoesNotContain("MAX(max_dop) AS max_dop", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBadActorText_NarrowsToTheFindingsDatabase_AndTheTopCpuTextBindsOnlyServerAndWindow()
    {
        /* #8317: $4 is the hash and $5 the finding's database; an empty $5 keeps every database the hash ran in. */
        var badActor = CommonText("BadActorDetail");
        Assert.Contains("AND   query_hash = $4", badActor, StringComparison.Ordinal);
        Assert.Contains("AND   ($5 = '' OR database_name = $5)", badActor, StringComparison.Ordinal);

        /* The ranked read binds $1 server, $2 and $3 window, and both readers add exactly those three. */
        var topCpu = CommonText("TopCpuQueries");
        Assert.Contains("WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3", topCpu, StringComparison.Ordinal);
        Assert.DoesNotContain("$4", topCpu, StringComparison.Ordinal);
        Assert.DoesNotContain("$5", topCpu, StringComparison.Ordinal);
    }
}
