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
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582: the live proof that the single-scan Query Store RankedTimeSeries text returns what the two-scan text returned, and
/// that it reads the wide table once. Every case compiles the SAME panel through <see cref="QueryStoreRankedHarness.Product"/> and
/// <see cref="QueryStoreRankedHarness.Oracle"/> (<c>CompileCore(..., false)</c>) over the rows of
/// <see cref="QueryStoreRankedHarness.SeedAsync"/> and asserts equal result sets. The harness is the reusable half: part 3 passes
/// its own compiler as the candidate.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it, so it cannot race
   live collection. */
public sealed class ComposeQueryStoreRankedSingleScanLiveTests
{
    private const string SkipMessage = "Set DARLING_TEST_PG to a Postgres connection string to run the #5582 single-scan RankedTimeSeries live tests.";

    private sealed record Window(string Name, DateTime Start, DateTime End, string Bucket);

    private static readonly Window[] Windows =
    {
        new("72 hours, hour grain", QueryStoreRankedHarness.WindowStart, QueryStoreRankedHarness.WindowEnd, "hour"),
        new("72 hours, day grain", QueryStoreRankedHarness.WindowStart, QueryStoreRankedHarness.WindowEnd, "day"),
        new("90 minutes, minute grain", QueryStoreRankedHarness.WindowStart.AddHours(10), QueryStoreRankedHarness.WindowStart.AddHours(10).AddMinutes(90), "minute"),
        new("30 minutes, one hour bucket", QueryStoreRankedHarness.WindowStart.AddHours(10), QueryStoreRankedHarness.WindowStart.AddHours(10).AddMinutes(30), "hour"),
        new("72 hours, minute grain (past the bucket bound)", QueryStoreRankedHarness.WindowStart, QueryStoreRankedHarness.WindowEnd, "minute"),
    };

    /// <summary>(measure key kind, key, aggregate or null for a ratio).</summary>
    private static readonly (string Kind, string Key, string? Aggregate)[] Measures =
    {
        ("measure", "qs_executions", "sum"), ("measure", "qs_executions", "avg"), ("measure", "qs_executions", "min"), ("measure", "qs_executions", "max"),
        ("measure", "qs_max_duration_us", "avg"), ("measure", "qs_max_duration_us", "min"), ("measure", "qs_max_duration_us", "max"),
        ("measure", "qs_max_cpu_us", "avg"), ("measure", "qs_max_cpu_us", "min"), ("measure", "qs_max_cpu_us", "max"),
        ("ratio", "qs_avg_duration_us", null), ("ratio", "qs_avg_cpu_us", null), ("ratio", "qs_total_duration_us", null), ("ratio", "qs_total_cpu_us", null),
    };

    /// <summary>(group columns, topN, whether the single scan is expected on a window within the bucket bound).</summary>
    private static readonly (string[] Groups, int TopN, bool Bounded)[] Groupings =
    {
        (new[] { "module_name" }, 4, true),
        (new[] { "database_name" }, 2, true),
        (new[] { "database_name", "module_name" }, 6, true),
        (new[] { "server" }, 2, true),
        (new[] { "query_hash" }, 5, false),
    };

    private static string PanelJson((string Kind, string Key, string? Aggregate) measure, string[] groups, int topN, string bucket, bool includeOther, string? filters = null) =>
        "{\"source\":\"query_store_stats\",\"" + measure.Kind + "\":\"" + measure.Key + "\""
        + (measure.Aggregate is null ? string.Empty : ",\"aggregate\":\"" + measure.Aggregate + "\"")
        + ",\"timeBucket\":\"" + bucket + "\",\"topN\":" + topN.ToString(CultureInfo.InvariantCulture)
        + ",\"groupBy\":[" + string.Join(",", groups.Select(g => "\"" + g + "\"")) + "],\"viz\":\"line\""
        + (includeOther ? ",\"includeOther\":true" : string.Empty) + (filters is null ? string.Empty : ",\"filters\":" + filters) + "}";

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> OpenAsync(string baseCs, System.Threading.CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(baseCs, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await QueryStoreRankedHarness.SeedAsync(connection, ct);
        return (scratch, connection);
    }

    [Fact]
    public async Task SingleScan_EqualsTheTwoScanText_ForEveryMeasureAggregateGroupingAndWindow()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await OpenAsync(baseCs!, ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        var compared = 0;
        var nonEmpty = 0;
        var tiesDiffered = 0;
        var singleScanCompiles = 0;
        foreach (var window in Windows)
        {
            var context = QueryStoreRankedHarness.WideContext(window.Start, window.End);
            foreach (var measure in Measures)
            {
                foreach (var (groups, topN, bounded) in Groupings)
                {
                    foreach (var includeOther in new[] { false, true })
                    {
                        var json = PanelJson(measure, groups, topN, window.Bucket, includeOther);
                        var result = await QueryStoreRankedHarness.CompareAsync(
                            connection, json, context, QueryStoreRankedHarness.Product, QueryStoreRankedHarness.Oracle, 0d, ct);
                        compared++;
                        nonEmpty += result.OracleRows > 0 ? 1 : 0;
                        tiesDiffered += result.MemberSetsDiffered ? 1 : 0;
                        Assert.True(result.Equal, $"{window.Name} | {json}: {result.Difference}");
                        Assert.True(result.MemberSetsDiffered || result.OracleRows == result.CandidateRows, $"{window.Name} | {json}: row counts {result.CandidateRows} vs {result.OracleRows}");

                        var usedSingleScan = result.CandidateSql.Contains("rank_base AS (", StringComparison.Ordinal);
                        var expectSingleScan = bounded && !window.Name.Contains("past the bucket bound", StringComparison.Ordinal);
                        Assert.True(expectSingleScan == usedSingleScan, $"{window.Name} | {json}: single scan expected={expectSingleScan}, compiled={usedSingleScan}");
                        singleScanCompiles += usedSingleScan ? 1 : 0;
                        Assert.False(result.OracleSql.Contains("rank_base", StringComparison.Ordinal), "the oracle must be the two-scan text");
                    }
                }
            }
        }

        Assert.True(compared >= 500, $"expected the full matrix; compared {compared}");
        Assert.True(nonEmpty * 10 >= compared * 8, $"the seed must give most cases rows: {nonEmpty} of {compared}");
        Assert.True(singleScanCompiles >= compared / 2, $"the single scan must be what most of the matrix ran: {singleScanCompiles} of {compared}");

        /* The seed has no tie at any of these cutoffs except by accident; report rather than require. */
        Assert.True(tiesDiffered <= compared / 20, $"member sets differed in {tiesDiffered} of {compared} cases (ties at the cutoff are the only explanation allowed)");

        var scoped = QueryStoreRankedHarness.WideContext(QueryStoreRankedHarness.WindowStart, QueryStoreRankedHarness.WindowEnd, new[] { QueryStoreRankedHarness.Servers[1].Name });
        foreach (var measure in Measures)
        {
            var json = PanelJson(measure, new[] { "module_name" }, 3, "hour", true,
                "[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"db1\"}]");
            var result = await QueryStoreRankedHarness.CompareAsync(
                connection, json, scoped, QueryStoreRankedHarness.Product, QueryStoreRankedHarness.Oracle, 0d, ct);
            Assert.True(result.Equal, $"scoped + filtered | {json}: {result.Difference}");
            Assert.True(result.OracleRows > 0, $"scoped + filtered | {json}: the seed returned nothing");
            Assert.Contains("rank_base AS (", result.CandidateSql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task SingleScan_AtTheCutoffTie_ChoosesAValidTopN_AndEveryChosenGroupsRowsAreEqual()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await OpenAsync(baseCs!, ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        var context = QueryStoreRankedHarness.WideContext(QueryStoreRankedHarness.WindowStart, QueryStoreRankedHarness.WindowEnd);
        const string tbFilter = "[{\"dimension\":\"module_name\",\"op\":\"like\",\"value\":\"tb%\"}]";
        var differed = 0;
        var cases = 0;
        foreach (var measure in Measures)
        {
            foreach (var topN in new[] { 1, 3, 4, 5, 9 })
            {
                var json = PanelJson(measure, new[] { "module_name" }, topN, "hour", false, tbFilter);
                var result = await QueryStoreRankedHarness.CompareAsync(
                    connection, json, context, QueryStoreRankedHarness.Product, QueryStoreRankedHarness.Oracle, 0d, ct);
                cases++;
                differed += result.MemberSetsDiffered ? 1 : 0;
                Assert.True(result.Equal, $"{json}: {result.Difference}");
                Assert.Contains("rank_base AS (", result.CandidateSql, StringComparison.Ordinal);
                Assert.True(result.OracleRows > 0, json);
            }
        }

        /* How often the two texts picked different tied members is the plan's choice, so it is not asserted; each case above is
           valid either way. */
        Assert.Equal(70, cases);
        Assert.True(differed >= 0);
    }

    /// <summary>
    /// The Avg partials are the numeric sum and the non-null count; their quotient over any slicing of a group must be the numeric
    /// <c>AVG(bigint)</c> gives, to the last digit, including sums past a bigint, NULLs, an all-NULL group and a one-row group.
    /// </summary>
    [Fact]
    public async Task AvgPartials_EqualAvgOfBigint_ExactlyAsNumeric_ForRandomData()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        for (var round = 1; round <= 6; round++)
        {
            var seed = round / 10.0;
            await using (var setup = new NpgsqlCommand(
                @"SELECT setseed(@seed);
DROP TABLE IF EXISTS avg_probe;
CREATE TEMP TABLE avg_probe AS
SELECT g, slice, v FROM
(
    SELECT (random() * 200)::integer AS g, (random() * 9)::integer AS slice,
           CASE WHEN random() < 0.1 THEN NULL
                WHEN random() < 0.5 THEN ((random() * 2 - 1) * 9.0e18)::bigint
                ELSE ((random() * 2000 - 1000)::bigint) END AS v
    FROM generate_series(1, 20000)
) AS t
UNION ALL SELECT 201, s, NULL::bigint FROM generate_series(0, 3) AS s
UNION ALL SELECT 202, 0, 5::bigint
UNION ALL SELECT 203, 0, 9223372036854775807::bigint
UNION ALL SELECT 203, 1, 9223372036854775806::bigint
UNION ALL SELECT 204, 0, (-9223372036854775807)::bigint
UNION ALL SELECT 204, 1, (-9223372036854775807)::bigint
UNION ALL SELECT 204, 2, 1::bigint;", connection))
            {
                setup.Parameters.AddWithValue("seed", seed);
                await setup.ExecuteNonQueryAsync(ct);
            }

            await using var compare = new NpgsqlCommand(
                @"WITH parts AS (SELECT g, slice, SUM(v) AS s, COUNT(v) AS c FROM avg_probe GROUP BY g, slice),
combined AS
(
    SELECT g, SUM(s) / NULLIF(SUM(c), 0) AS a, CAST(SUM(s) / NULLIF(SUM(c), 0) AS double precision) AS d FROM parts GROUP BY g
),
direct AS (SELECT g, AVG(v) AS a, CAST(AVG(v) AS double precision) AS d FROM avg_probe GROUP BY g)
SELECT count(*)::integer,
       count(*) FILTER (WHERE c.g IS NULL OR d.g IS NULL OR c.a::text IS DISTINCT FROM d.a::text OR c.d IS DISTINCT FROM d.d)::integer,
       count(*) FILTER (WHERE d.a IS NULL)::integer
FROM combined AS c FULL JOIN direct AS d ON d.g = c.g;", connection);
            await using var reader = await compare.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.True(reader.GetInt32(0) >= 200, $"round {round}: the probe has too few groups");
            Assert.True(reader.GetInt32(2) >= 1, $"round {round}: the all-NULL group must be in the probe");
            Assert.True(reader.GetInt32(1) == 0, $"round {round} (seed {seed}): {reader.GetInt32(1)} groups where the combined partials differ from AVG(bigint)");
        }
    }

    /// <summary>
    /// The plan of the single-scan text reads the wide table once; the oracle's reads it twice. Counted over the scans that
    /// carry the fact alias, per relation (the table, or each of its chunks once). Red-planted by compiling the old text under
    /// the new switch: the first assertion then fails.
    /// </summary>
    [Fact]
    public async Task SingleScan_ReadsTheWideTableOnce_AndTheOracleReadsItTwice()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await OpenAsync(baseCs!, ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        foreach (var (bucket, groups) in new[] { ("hour", new[] { "module_name" }), ("day", new[] { "database_name", "module_name" }) })
        {
            var json = PanelJson(("ratio", "qs_total_cpu_us", null), groups, 5, bucket, false);
            var plan = QueryStoreRankedHarness.Parse(json);
            var context = QueryStoreRankedHarness.WideContext(QueryStoreRankedHarness.WindowStart, QueryStoreRankedHarness.WindowEnd);

            var single = await ScansPerRelationAsync(connection, QueryStoreRankedHarness.Product, plan, context, ct);
            Assert.True(single.Count > 0 && single.Values.All(n => n == 1),
                $"{bucket}: the single-scan text must scan each wide relation once: {Describe(single)}");

            var oracle = await ScansPerRelationAsync(connection, QueryStoreRankedHarness.Oracle, plan, context, ct);
            Assert.True(oracle.Count > 0 && oracle.Values.All(n => n == 2),
                $"{bucket}: the oracle must scan each wide relation twice (else this test counts nothing): {Describe(oracle)}");
        }
    }

    private static string Describe(Dictionary<string, int> scans) => string.Join(", ", scans.Select(kv => kv.Key + "=" + kv.Value));

    private static async Task<Dictionary<string, int>> ScansPerRelationAsync(
        NpgsqlConnection connection, QueryStoreRankedHarness.Compiler compiler, PanelPlan plan, ComposeRunContext context, System.Threading.CancellationToken ct)
    {
        var (compiled, error) = compiler(plan, context);
        Assert.True(error is null, error);
        await using var command = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + compiled!.Sql, connection);
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        var planJson = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(planJson);
        var scans = new Dictionary<string, int>(StringComparer.Ordinal);
        void Walk(JsonElement node)
        {
            if (node.TryGetProperty("Relation Name", out var relation) && node.TryGetProperty("Alias", out var alias)
                && alias.GetString()!.StartsWith("w", StringComparison.Ordinal)
                && (relation.GetString()!.Contains("query_store_interval_wide", StringComparison.Ordinal) || relation.GetString()!.StartsWith("_hyper_", StringComparison.Ordinal)))
            {
                scans[relation.GetString()!] = scans.GetValueOrDefault(relation.GetString()!) + 1;
            }

            if (node.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Walk(child);
                }
            }
        }

        Walk(document.RootElement[0].GetProperty("Plan"));
        return scans;
    }
}
