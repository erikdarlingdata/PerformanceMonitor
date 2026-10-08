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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582 part 3, lane 4: the stamp route answers EXACTLY what the wide-table route answers. The lane 1 seed (30 hours, three servers,
/// NULLs, weighted products past a bigint) is built hour by hour; every Query Store measure x valid aggregate runs through every panel
/// mode, once with the runner's StampThrough (the candidate) and once without it (the oracle, today's wide-table text), and the two
/// result sets must be equal bit for bit (<see cref="QueryStoreRankedHarness"/>, tolerance 0). Modes: Scalar, Ranked by module and by
/// query_hash, TimeSeries at minute, hour and day, RankedTimeSeries with includeOther on and off, on the single-scan text and on the
/// two-scan text. The window starts mid-hour; one window ends inside the first unbuilt hours (the rollup, then a tail of wide rows),
/// the other lies wholly in built hours (StampThrough capped at the window end). Each shape also runs server-scoped and filtered.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it, so it never touches the shared database and cannot race the
   live collection. */
public sealed class ComposeQueryStoreStampExactnessLiveTests
{
    private static readonly (string Kind, string Key, string? Aggregate)[] Measures =
    {
        ("measure", "qs_executions", "sum"), ("measure", "qs_executions", "avg"), ("measure", "qs_executions", "min"), ("measure", "qs_executions", "max"),
        ("measure", "qs_max_duration_us", "avg"), ("measure", "qs_max_duration_us", "min"), ("measure", "qs_max_duration_us", "max"),
        ("measure", "qs_max_cpu_us", "avg"), ("measure", "qs_max_cpu_us", "min"), ("measure", "qs_max_cpu_us", "max"),
        ("ratio", "qs_avg_duration_us", null), ("ratio", "qs_avg_cpu_us", null), ("ratio", "qs_total_duration_us", null), ("ratio", "qs_total_cpu_us", null),
    };

    private static string Panel(
        (string Kind, string Key, string? Aggregate) measure, string viz, string[]? groups, int? topN, string? bucket, bool includeOther = false, string? filters = null) =>
        "{\"source\":\"query_store_stats\",\"" + measure.Kind + "\":\"" + measure.Key + "\""
        + (measure.Aggregate is null ? string.Empty : ",\"aggregate\":\"" + measure.Aggregate + "\"")
        + (bucket is null ? string.Empty : ",\"timeBucket\":\"" + bucket + "\"")
        + (topN is null ? string.Empty : ",\"topN\":" + topN.Value.ToString(CultureInfo.InvariantCulture))
        + (groups is null ? string.Empty : ",\"groupBy\":[" + string.Join(",", groups.Select(g => "\"" + g + "\"")) + "]")
        + ",\"viz\":\"" + viz + "\""
        + (includeOther ? ",\"includeOther\":true" : string.Empty)
        + (filters is null ? string.Empty : ",\"filters\":" + filters) + "}";

    private const string Db1 = "[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"db1\"}]";

    private sealed record Counter(int Compared, int WithRows, int StampText);

    private static async Task<Counter> CompareAsync(
        NpgsqlConnection connection, string json, ComposeRunContext withStamp, ComposeRunContext without, string label, Counter counter, CancellationToken ct)
    {
        var compared = counter.Compared;
        var withRows = counter.WithRows;
        var stampText = counter.StampText;
        foreach (var candidate in new[] { QueryStoreRankedHarness.Product, QueryStoreRankedHarness.Oracle })
        {
            var result = await QueryStoreRankedHarness.CompareAsync(connection, json, withStamp, candidate, without, QueryStoreRankedHarness.Oracle, 0d, ct);
            compared++;
            withRows += result.OracleRows > 0 ? 1 : 0;
            Assert.True(result.Equal, $"{label} | {json}: {result.Difference}");
            Assert.True(result.MemberSetsDiffered || result.OracleRows == result.CandidateRows, $"{label} | {json}: row counts {result.CandidateRows} vs {result.OracleRows}");
            Assert.True(result.CandidateSql.Contains("query_store_compose_stamp", StringComparison.Ordinal), $"{label} | {json}: the candidate did not read the rollup");
            Assert.False(result.OracleSql.Contains("query_store_compose_stamp", StringComparison.Ordinal), $"{label} | {json}: the oracle must be the wide-table text");
            stampText++;
        }

        return new Counter(compared, withRows, stampText);
    }

    [Fact]
    public async Task TheStampRoute_EqualsTheWideRoute_ForEveryMeasureAggregateAndMode_BitForBit()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* Window A: starts at hourNow - 28 h + 17 min (mid-hour, inside the built run), ends at hourNow - 40 min. The built hours stop at
               hourNow - 3 h, so the rollup answers up to hourNow - 2 h and the rest is the wide-table tail. */
            var startA = hourNow.AddHours(-28).AddMinutes(17);
            var endA = hourNow.AddMinutes(-40);
            var throughA = await ComposeStampLiveSupport.StampThroughAsync(connection, startA, endA, ct);
            Assert.Equal(hourNow.AddHours(-2), throughA);

            /* Window B: 90 minutes inside built hours, starting mid-hour: nothing is unbuilt, so StampThrough is capped at the window end. */
            var startB = hourNow.AddHours(-20).AddMinutes(17);
            var endB = startB.AddMinutes(90);
            var throughB = await ComposeStampLiveSupport.StampThroughAsync(connection, startB, endB, ct);
            Assert.Equal(endB, throughB);

            var counter = new Counter(0, 0, 0);
            foreach (var scope in new IReadOnlyList<string>?[] { null, new[] { "srv2" } })
            {
                foreach (var filters in new string?[] { null, Db1 })
                {
                    var label = $"scope={(scope is null ? "fleet" : "srv2")} filter={(filters is null ? "none" : "db1")}";
                    foreach (var (start, end, through, buckets) in new[]
                    {
                        (startA, endA, throughA, new[] { "hour", "day" }),
                        (startB, endB, throughB, new[] { "minute", "hour" }),
                    })
                    {
                        var withStamp = QueryStoreRankedHarness.WideContext(start, end, scope) with { QueryStoreStampThrough = through };
                        var without = QueryStoreRankedHarness.WideContext(start, end, scope);
                        var windowLabel = $"{label} window={start:HH:mm}..{end:HH:mm}";
                        foreach (var measure in Measures)
                        {
                            counter = await CompareAsync(connection, Panel(measure, "stat", null, null, null, false, filters), withStamp, without, windowLabel, counter, ct);
                            foreach (var group in new[] { "module_name", "query_hash" })
                            {
                                counter = await CompareAsync(connection, Panel(measure, "bar", new[] { group }, 4, null, false, filters), withStamp, without, windowLabel, counter, ct);
                            }

                            foreach (var bucket in buckets)
                            {
                                counter = await CompareAsync(connection, Panel(measure, "line", null, null, bucket, false, filters), withStamp, without, windowLabel, counter, ct);
                                foreach (var (groups, topN) in new[] { (new[] { "module_name" }, 3), (new[] { "database_name", "module_name" }, 5), (new[] { "query_hash" }, 4) })
                                {
                                    foreach (var includeOther in new[] { false, true })
                                    {
                                        counter = await CompareAsync(connection, Panel(measure, "line", groups, topN, bucket, includeOther, filters), withStamp, without, windowLabel, counter, ct);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Assert.True(counter.Compared >= 1500, $"expected the full matrix; compared {counter.Compared}");
            Assert.Equal(counter.Compared, counter.StampText);
            Assert.True(counter.WithRows * 10 >= counter.Compared * 8, $"the seed must give most cases rows: {counter.WithRows} of {counter.Compared}");
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }
}
