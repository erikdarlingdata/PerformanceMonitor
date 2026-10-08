/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582 part 3: compile-level pins for the Query Store stamp route. When the runner supplies
/// <see cref="ComposeRunContext.QueryStoreStampThrough"/> and every partial the panel needs has a rollup column, the wide read's
/// fact relation becomes ONE FROM item of three <c>UNION ALL</c> arms (the rollup, the stale pairs, the tail) over partial rows,
/// and every value is the combine form over the partial columns. Nothing here opens a connection; the live proof that these texts
/// return the wide-table numbers is the lane 4 live classes.
/// </summary>
public sealed class ComposeQueryStoreStampCompileTests
{
    private static readonly DateTime Day = new(2026, 10, 7, 0, 0, 0, DateTimeKind.Unspecified);

    private static readonly Regex s_relation = new(@"FROM \((SELECT f\.server_id, .*?)\) AS f\r?\n", RegexOptions.Singleline | RegexOptions.CultureInvariant);

    private static ComposeRunContext Context(
        DateTime start, DateTime end, DateTime? stampThrough, DateTime? wideStart = null, bool wide = true, IReadOnlyList<string>? servers = null) =>
        new(servers, start, end, ComposeRunContext.NoVariables, RollupAvailability.None, end, RollupCoverage.Unknown,
            QueryStoreWideEligible: wide, QueryStoreWideStart: wideStart, QueryStoreGroupMembers: 10, QueryStoreStampThrough: stampThrough);

    private static ComposeCompiled Compile(
        string json, ComposeRunContext context, bool singleScan = true, Func<string, string?>? stampColumns = null)
    {
        var plan = QueryStoreRankedHarness.Parse(json);
        var (compiled, error) = ComposeCompiler.CompileCore(plan, context, singleScan, stampColumns);
        Assert.True(error is null, error);
        return compiled!;
    }

    private static string Panel(string measureKey, string? aggregate, string shape)
    {
        var kind = MeasureCatalog.Measure(measureKey)!.Kind == MeasureKind.Ratio ? "ratio" : "measure";
        var aggregateJson = aggregate is null ? string.Empty : $",\"aggregate\":\"{aggregate}\"";
        return $"{{\"source\":\"query_store_stats\",\"{kind}\":\"{measureKey}\"{aggregateJson}{shape}}}";
    }

    private static string TopQueriesOverTime(string measure = "qs_total_cpu_us") =>
        Panel(measure, null, ",\"timeBucket\":\"minute\",\"topN\":10,\"groupBy\":[\"query_hash\"],\"viz\":\"line\"");

    private static int Occurrences(string text, string needle) =>
        (text.Length - text.Replace(needle, string.Empty, StringComparison.Ordinal).Length) / needle.Length;

    /// <summary>The fact relations of a statement, one per scan of the fact rows.</summary>
    private static List<string> Relations(string sql) =>
        s_relation.Matches(sql).Select(m => m.Groups[1].Value).ToList();

    [Fact]
    public void TheStampRoute_ReadsThreeArms_WithTheDesignsBounds()
    {
        var through = Day.AddHours(20);
        var compiled = Compile(TopQueriesOverTime(), Context(Day, Day.AddDays(1), through));
        var relations = Relations(compiled.Sql);
        Assert.Equal(2, relations.Count);   /* query_hash is unbounded, so the rank and the series each scan */
        Assert.Equal(relations[0], relations[1]);

        var relation = relations[0];
        var arms = relation.Split(" UNION ALL ", StringSplitOptions.None);
        Assert.Equal(3, arms.Length);

        /* Parameters: $1 start, $2 end, $3 stampThrough (no wide start bound: it is not later than the window), $4 topN. */
        Assert.Equal(4, compiled.Parameters.Count);
        Assert.Equal(DateTime.SpecifyKind(through, DateTimeKind.Unspecified), compiled.Parameters[2].Value);

        var rollup = arms[0];
        Assert.Contains("FROM collect.query_store_compose_stamp AS f JOIN collect.servers AS s ON s.server_id = f.server_id", rollup, StringComparison.Ordinal);
        Assert.Contains("f.collection_time >= $1 AND f.collection_time < $3", rollup, StringComparison.Ordinal);
        Assert.Contains(
            "EXISTS (SELECT 1 FROM collect.query_store_compose_stamp_built AS b WHERE b.server_id = f.server_id AND b.hour = date_trunc('hour', f.collection_time) AND b.built_seq = b.late_seq)",
            rollup, StringComparison.Ordinal);

        var stale = arms[1];
        Assert.Contains("FROM collect.query_store_compose_stamp_built AS b CROSS JOIN LATERAL (SELECT w.* FROM collect.query_store_interval_wide AS w", stale, StringComparison.Ordinal);
        Assert.Contains("w.server_id = b.server_id AND w.collection_time >= b.hour AND w.collection_time < b.hour + interval '1 hour'", stale, StringComparison.Ordinal);
        Assert.Contains("w.collection_time >= $1 AND w.collection_time < $3 OFFSET 0) AS f", stale, StringComparison.Ordinal);
        Assert.Contains("b.hour >= date_trunc('hour', $1) AND b.hour < $3 AND b.built_seq IS DISTINCT FROM b.late_seq", stale, StringComparison.Ordinal);

        /* The tail starts at $stampThrough (never the window start) and ends at the window end, inclusive as the wide read is. */
        var tail = arms[2];
        Assert.Contains("FROM collect.query_store_interval_wide AS f JOIN collect.servers AS s ON s.server_id = f.server_id", tail, StringComparison.Ordinal);
        Assert.EndsWith("WHERE f.collection_time >= $3 AND f.collection_time <= $2", tail, StringComparison.Ordinal);
        Assert.DoesNotContain("f.collection_time >= $1", tail, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWideStart_WhenLaterThanTheWindow_BoundsTheRollupAndTheStaleArms_AndTheTailStillStartsAtStampThrough()
    {
        var wideStart = Day.AddHours(3);
        var compiled = Compile(TopQueriesOverTime(), Context(Day, Day.AddDays(1), Day.AddHours(20), wideStart));
        var relation = Relations(compiled.Sql)[0];
        var arms = relation.Split(" UNION ALL ", StringSplitOptions.None);

        /* $1 start, $2 end, $3 wideStart, $4 stampThrough. */
        Assert.Contains("f.collection_time >= $3 AND f.collection_time < $4", arms[0], StringComparison.Ordinal);
        Assert.Contains("w.collection_time >= $3 AND w.collection_time < $4 OFFSET 0) AS f", arms[1], StringComparison.Ordinal);
        Assert.Contains("b.hour >= date_trunc('hour', $3) AND b.hour < $4", arms[1], StringComparison.Ordinal);
        Assert.EndsWith("WHERE f.collection_time >= $4 AND f.collection_time <= $2", arms[2], StringComparison.Ordinal);
        Assert.Equal(5, compiled.Parameters.Count);
    }

    [Fact]
    public void TheServerScope_AndThePushableFilters_GoIntoAllThreeArms()
    {
        var json = Panel("qs_executions", "sum", ",\"timeBucket\":\"hour\",\"filters\":[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"Sales\"},{\"dimension\":\"server\",\"op\":\"eq\",\"value\":\"srv-a\"}],\"viz\":\"line\"");
        var compiled = Compile(json, Context(Day, Day.AddDays(1), Day.AddHours(12), servers: new[] { "srv-a", "srv-b" }));
        var arms = Relations(compiled.Sql)[0].Split(" UNION ALL ", StringSplitOptions.None);
        Assert.Equal(3, arms.Length);
        foreach (var arm in arms)
        {
            Assert.Contains("s.server_id = ANY(ARRAY(SELECT reg.server_id FROM collect.servers AS reg WHERE reg.server_name = ANY($3)))", arm, StringComparison.Ordinal);
            Assert.Contains("f.database_name = ", arm, StringComparison.Ordinal);
            Assert.Contains("s.server_name = ", arm, StringComparison.Ordinal);
            Assert.DoesNotContain("f.server_name", arm, StringComparison.Ordinal);
        }

        /* #5582 part 3: the stale arm also scopes the LEDGER row, so a stale pair of a server out of scope costs no wide-table probe (found
           on a live plan: the scope over the registry join alone is applied after the lateral). */
        Assert.Contains("AND b.server_id = ANY(ARRAY(SELECT reg.server_id FROM collect.servers AS reg WHERE reg.server_name = ANY($3)))", arms[1], StringComparison.Ordinal);
        Assert.DoesNotContain("b.server_id = ANY", arms[0], StringComparison.Ordinal);
        Assert.DoesNotContain("b.server_id = ANY", arms[2], StringComparison.Ordinal);

        /* The scope is bound once and the stamp bound is the next parameter after it is not: window, stampThrough, scope, filters. */
        Assert.Equal(6, compiled.Parameters.Count);
    }

    [Fact]
    public void TheStampMode_ReadsNoRawMeasureColumnOutsideTheRelation()
    {
        foreach (var measure in MeasureCatalog.Measures.Where(m => m.SourceTable == "query_store_stats"))
        {
            var agg = measure.Kind == MeasureKind.Ratio ? null : MeasureCatalog.WireName(measure.ValidAggs[0]);
            var compiled = Compile(Panel(measure.Key, agg, ",\"viz\":\"stat\""), Context(Day, Day.AddDays(1), Day.AddHours(12)));
            var outside = s_relation.Replace(compiled.Sql, "FROM (RELATION) AS f\n");
            foreach (var raw in new[] { "execution_count", "avg_duration_us", "avg_cpu_time_us", "max_duration_us", "max_cpu_time_us" })
            {
                Assert.True(!outside.Contains(raw, StringComparison.Ordinal), $"{measure.Key}: the statement outside the relation still reads {raw}: {outside}");
            }
        }
    }

    public static IEnumerable<object[]> NoStampCases()
    {
        var through = Day.AddHours(20);
        yield return new object[] { "StampThroughNull", Context(Day, Day.AddDays(1), null), TopQueriesOverTime(), null! };
        yield return new object[] { "StampThroughAtTheWindowStart", Context(Day, Day.AddDays(1), Day), TopQueriesOverTime(), null! };
        yield return new object[] { "StampThroughBeforeTheWindowStart", Context(Day, Day.AddDays(1), Day.AddHours(-2)), TopQueriesOverTime(), null! };
        yield return new object[] { "StampThroughAtTheWideStart", Context(Day, Day.AddDays(1), Day.AddHours(5), Day.AddHours(5)), TopQueriesOverTime(), null! };
        yield return new object[] { "StampThroughBeforeTheWideStart", Context(Day, Day.AddDays(1), Day.AddHours(5), Day.AddHours(6)), TopQueriesOverTime(), null! };
        yield return new object[] { "NotTheWideRoute", Context(Day, Day.AddDays(1), through, wide: false), TopQueriesOverTime(), null! };
        yield return new object[]
        {
            "NotQueryStore",
            Context(Day, Day.AddDays(1), through),
            "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"topN\":5,\"groupBy\":[\"wait_type\"],\"viz\":\"line\"}",
            null!,
        };
        yield return new object[]
        {
            "APartialHasNoColumn",
            Context(Day, Day.AddDays(1), through),
            TopQueriesOverTime(),
            (Func<string, string?>)(e => e == "SUM(f.avg_cpu_time_us * f.execution_count)" ? null : ComposeCompiler.StampColumnFor(e)),
        };
    }

    [Theory]
    [MemberData(nameof(NoStampCases))]
    public void NoStampText_WhenAnyOfTheGatesFails_AndTheWideTextIsTheUnstampedCompile(
        string name, ComposeRunContext context, string json, Func<string, string?>? map)
    {
        var compiled = Compile(json, context, stampColumns: map);
        Assert.DoesNotContain("query_store_compose_stamp", compiled.Sql, StringComparison.Ordinal);

        /* Byte for byte the compile that carries no stamp instant at all. */
        var unstamped = Compile(json, context with { QueryStoreStampThrough = null });
        Assert.Equal(unstamped.Sql, compiled.Sql);
        Assert.Equal(unstamped.Parameters.Count, compiled.Parameters.Count);
        Assert.NotEmpty(name);
    }

    [Fact]
    public void TheRelation_SelectsTheKeyColumnsThenThePartials()
    {
        var compiled = Compile(TopQueriesOverTime(), Context(Day, Day.AddDays(1), Day.AddHours(12)));
        var relation = Relations(compiled.Sql)[0];
        Assert.Contains("SELECT f.server_id, f.database_name, f.module_name, f.query_hash, f.collection_time, s.server_name", relation, StringComparison.Ordinal);
    }

    public static IEnumerable<object[]> EveryMeasureAggregateAndMode()
    {
        var shapes = new (string Mode, string Json)[]
        {
            ("Scalar", ",\"viz\":\"stat\""),
            ("TimeSeries", ",\"timeBucket\":\"hour\",\"viz\":\"line\""),
            ("Ranked", ",\"topN\":5,\"groupBy\":[\"database_name\"],\"viz\":\"bar\""),
            ("RankedTimeSeries", ",\"timeBucket\":\"hour\",\"topN\":5,\"groupBy\":[\"database_name\"],\"viz\":\"line\""),
            ("RankedTimeSeriesOther", ",\"timeBucket\":\"hour\",\"topN\":5,\"groupBy\":[\"database_name\"],\"includeOther\":true,\"viz\":\"line\""),
            ("RankedTimeSeriesQueryHash", ",\"timeBucket\":\"hour\",\"topN\":5,\"groupBy\":[\"query_hash\"],\"viz\":\"line\""),
        };

        foreach (var measure in MeasureCatalog.Measures.Where(m => m.SourceTable == "query_store_stats"))
        {
            var aggregates = measure.Kind == MeasureKind.Ratio
                ? new string?[] { null }
                : measure.ValidAggs.Select(a => (string?)MeasureCatalog.WireName(a)).ToArray();
            foreach (var aggregate in aggregates)
            {
                foreach (var (mode, json) in shapes)
                {
                    yield return new object[] { measure.Key, aggregate!, mode, json };
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(EveryMeasureAggregateAndMode))]
    public void EveryQueryStoreMeasureAndAggregate_CompilesInStampMode_AndTheRankAndTheSeriesReadTheSameRelation(
        string measure, string? aggregate, string mode, string shape)
    {
        var context = Context(Day, Day.AddDays(1), Day.AddHours(12));
        var compiled = Compile(Panel(measure, aggregate, shape), context);
        var sql = compiled.Sql;

        var relations = Relations(sql);
        Assert.NotEmpty(relations);
        Assert.All(relations, r => Assert.Equal(relations[0], r));
        Assert.Equal(3, relations[0].Split(" UNION ALL ", StringSplitOptions.None).Length);

        /* The same text names each fact scan: the single-scan RankedTimeSeries reads the relation once, every other two-scan shape twice. */
        var expectedScans = mode == "RankedTimeSeriesQueryHash" ? 2 : 1;
        Assert.Equal(expectedScans, relations.Count);
        Assert.Equal(expectedScans, Occurrences(sql, "collect.query_store_compose_stamp AS f"));
        Assert.Equal(expectedScans, Occurrences(sql, "collect.query_store_interval_wide AS f"));

        /* One new parameter beyond today's: $stampThrough. */
        var unstamped = Compile(Panel(measure, aggregate, shape), context with { QueryStoreStampThrough = null });
        Assert.Equal(unstamped.Parameters.Count + 1, compiled.Parameters.Count);
    }

    [Fact]
    public void TheOverlay_ReadsItsPartialsFromTheSameRelation_AndAnUnmappedOverlayPartialFallsBack()
    {
        var json = "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\","
            + "\"overlay\":{\"measure\":\"qs_max_duration_us\",\"aggregate\":\"max\"}}";
        var context = Context(Day, Day.AddDays(1), Day.AddHours(12));
        var compiled = Compile(json, context);
        Assert.Contains("CAST(SUM(f.ec_sum) AS double precision) AS value", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("MAX(f.maxdur_max)", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains(" AS value2", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains(", f.ec_sum, f.maxdur_max FROM collect.query_store_compose_stamp AS f", compiled.Sql, StringComparison.Ordinal);

        var fallback = Compile(json, context, stampColumns: e => e == "MAX(f.max_duration_us)" ? null : ComposeCompiler.StampColumnFor(e));
        Assert.DoesNotContain("query_store_compose_stamp", fallback.Sql, StringComparison.Ordinal);
        Assert.Equal(Compile(json, context with { QueryStoreStampThrough = null }).Sql, fallback.Sql);
    }

    [Fact]
    public void EveryPartialTheCompilerCanAsk_ForAQueryStoreMeasure_HasARollupColumn()
    {
        foreach (var measure in MeasureCatalog.Measures.Where(m => m.SourceTable == "query_store_stats"))
        {
            var aggregates = measure.Kind == MeasureKind.Ratio ? new string?[] { null } : measure.ValidAggs.Select(a => (string?)MeasureCatalog.WireName(a)).ToArray();
            foreach (var aggregate in aggregates)
            {
                var asked = new List<string>();
                var compiled = Compile(Panel(measure.Key, aggregate, ",\"viz\":\"stat\""), Context(Day, Day.AddDays(1), Day.AddHours(12)),
                    stampColumns: e => { asked.Add(e); return ComposeCompiler.StampColumnFor(e); });
                Assert.Contains("query_store_compose_stamp", compiled.Sql, StringComparison.Ordinal);
                Assert.NotEmpty(asked);
                Assert.All(asked, e => Assert.NotNull(ComposeCompiler.StampColumnFor(e)));
            }
        }
    }


    /// <summary>The stamp-mode text, byte for byte, for the "top 10 queries by CPU over time" panel of one fleet-day at minute grain: the
    /// panel part 2's oracle pin holds in its wide-table form, read through the rollup for the first 20 hours. The relation is the
    /// three arms; the rank and the series read the same text.</summary>
    [Fact]
    public void TheStampText_ForTheFleetDayTopQueriesPanel_IsPinned()
    {
        var sql = Compile(TopQueriesOverTime(), Context(Day, Day.AddDays(1), Day.AddHours(20))).Sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        const string Relation = "SELECT f.server_id, f.database_name, f.module_name, f.query_hash, f.collection_time, s.server_name, f.cpu_wsum FROM collect.query_store_compose_stamp AS f JOIN collect.servers AS s ON s.server_id = f.server_id WHERE f.collection_time >= $1 AND f.collection_time < $3 AND EXISTS (SELECT 1 FROM collect.query_store_compose_stamp_built AS b WHERE b.server_id = f.server_id AND b.hour = date_trunc('hour', f.collection_time) AND b.built_seq = b.late_seq) UNION ALL "
            + "SELECT f.server_id, f.database_name, f.module_name, f.query_hash, f.collection_time, s.server_name, CAST(f.avg_cpu_time_us * f.execution_count AS numeric) AS cpu_wsum FROM collect.query_store_compose_stamp_built AS b CROSS JOIN LATERAL (SELECT w.* FROM collect.query_store_interval_wide AS w WHERE w.server_id = b.server_id AND w.collection_time >= b.hour AND w.collection_time < b.hour + interval '1 hour' AND w.collection_time >= $1 AND w.collection_time < $3 OFFSET 0) AS f JOIN collect.servers AS s ON s.server_id = f.server_id WHERE b.hour >= date_trunc('hour', $1) AND b.hour < $3 AND b.built_seq IS DISTINCT FROM b.late_seq UNION ALL "
            + "SELECT f.server_id, f.database_name, f.module_name, f.query_hash, f.collection_time, s.server_name, CAST(f.avg_cpu_time_us * f.execution_count AS numeric) AS cpu_wsum FROM collect.query_store_interval_wide AS f JOIN collect.servers AS s ON s.server_id = f.server_id WHERE f.collection_time >= $3 AND f.collection_time <= $2";
        const string Expected = """
            WITH topn AS (
                SELECT f.query_hash AS query_hash, (CAST(SUM(f.cpu_wsum) AS double precision)) * 1.0 / 1000000.0 AS value
                FROM (<RELATION>) AS f
                WHERE f.collection_time >= $1
                  AND f.collection_time <= $2
                GROUP BY f.query_hash
                ORDER BY value DESC NULLS LAST
                LIMIT $4
            )
            SELECT * FROM (
            SELECT date_trunc('minute', f.collection_time) AS bucket, f.query_hash AS query_hash, (CAST(SUM(f.cpu_wsum) AS double precision)) * 1.0 / 1000000.0 AS value
            FROM (<RELATION>) AS f
            WHERE f.collection_time >= $1
              AND f.collection_time <= $2
              AND EXISTS (SELECT 1 FROM topn AS t WHERE t.query_hash IS NOT DISTINCT FROM f.query_hash)
            GROUP BY date_trunc('minute', f.collection_time), f.query_hash
            ORDER BY bucket DESC
            LIMIT 10000
            ) AS capped
            ORDER BY bucket
            """;
        Assert.Equal(Expected.Replace("\r\n", "\n", StringComparison.Ordinal).Replace("<RELATION>", Relation, StringComparison.Ordinal), sql);
    }
}
