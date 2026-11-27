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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres, so its seed is the only data the plan sees. */

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #8570: the plan fetch of <c>get_analysis_facts</c> (<see cref="PgFactCollector.PlanAdvisorySql"/>) keeps an in-window row when its own plan column, or its
/// digest's dimension row, carries a plan. A probe of <c>query_plan_dim</c> per snapshot row (85,451 for one field window) or per distinct digest (30,000 for
/// another, 25 s on a cold cache) pays for the whole window before the rank keeps ten. The statement probes the plans' costliest rows in cost order until ten
/// have a plan, and the rows of the plans whose costliest row has none. The test seeds many snapshot rows over few digests and pins two things by what the read
/// does: the ten rows returned are exactly the rows the per-row statement returns (including plan keys whose costliest row has no plan but a cheaper row does,
/// at the head of the walk and in its middle), and the dimension is probed fewer times than there are distinct digests, never per row and never scanned whole.
/// </summary>
public sealed class PlanAdvisoryDigestProbeLiveTests
{
    private const int ServerId = 85701;
    private const int Digests = 200;

    private const string InsertHead = @"INSERT INTO query_stats
                      (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                       query_text, query_plan_xml, query_plan_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                       delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                       min_dop, max_dop, sample_interval_seconds)
                  ";

    /// <summary>The per-row guard (a correlated EXISTS per snapshot row): the reference the plan fetch's results are held to.</summary>
    private const string PerRowGuardSql = @"
SELECT COALESCE(r.query_plan_xml, d.query_plan_xml) AS query_plan_xml, d.query_plan_gz
FROM
(
    SELECT query_plan_xml, query_plan_digest, delta_worker_time
    FROM
    (
        SELECT
            f.query_plan_xml,
            f.query_plan_digest,
            f.delta_worker_time,
            ROW_NUMBER() OVER (PARTITION BY f.database_name, f.query_hash, COALESCE(f.query_plan_hash, f.plan_handle) ORDER BY f.delta_worker_time DESC) AS plan_rank
        FROM query_stats AS f
        WHERE f.server_id = $1
        AND   f.collection_time >= $2
        AND   f.collection_time <= $3
        AND   (f.query_plan_xml IS NOT NULL
               OR EXISTS (SELECT 1 FROM query_plan_dim AS p
                          WHERE p.digest = f.query_plan_digest
                          AND   (p.query_plan_xml IS NOT NULL OR p.query_plan_gz IS NOT NULL)))
    ) AS ranked
    WHERE plan_rank = 1
    ORDER BY delta_worker_time DESC
    LIMIT 10
) AS r
LEFT JOIN query_plan_dim AS d ON d.digest = r.query_plan_digest
ORDER BY r.delta_worker_time DESC";

    [Fact]
    public Task TheFetch_ReturnsThePerRowStatementsRows_AndProbesOnlyTheRowsItMayReturn() =>
        WithSeedAsync(async (connection, start, end, ct) =>
        {
            var expected = await RowsAsync(connection, PerRowGuardSql, start, end, ct);
            var actual = await RowsAsync(connection, PgFactCollector.PlanAdvisorySql, start, end, ct);

            Assert.Equal(10, expected.Count);
            /* The same ten plans in the same order, content included. */
            Assert.Equal(expected, actual);

            /* A plan key whose costliest row has no plan stands for itself through its cheaper row, which does (key 1001: its plan is plan 3). */
            Assert.Contains("<ShowPlanXML>plan 3</ShowPlanXML>|<null>", actual);
            /* So does a plan key whose costliest row sits in the middle of the walk (key 1006: its plan is plan 4, which outranks the tenth bulk plan). */
            Assert.Contains("<ShowPlanXML>plan 4</ShowPlanXML>|<null>", actual);
            /* A gzip-only dimension row counts (key 1003: digest 12). */
            Assert.Contains(actual, row => row.StartsWith("<null>|", StringComparison.Ordinal) && !row.EndsWith("<null>", StringComparison.Ordinal));
            /* A row's own column counts with no digest at all (key 1005). */
            Assert.Contains("<ShowPlanXML>own column plan</ShowPlanXML>|<null>", actual);
            /* A digest whose dimension row is gone (key 1002) and one whose dimension row has no content (key 1004) are among the costliest keys but never appear. */
            Assert.DoesNotContain("<null>|<null>", actual);

            var oldProbes = await DimensionProbesAsync(connection, PerRowGuardSql, start, end, ct);
            var newProbes = await DimensionProbesAsync(connection, PgFactCollector.PlanAdvisorySql, start, end, ct);

            /* The seed must have the shape that costs: the per-row guard probes the dimension once per snapshot row that has no plan of its own. */
            Assert.True(oldProbes.Loops > 10_000, $"the seed's per-row statement probed the dimension {oldProbes.Loops} times, so it does not show the defect");
            /* Fewer probes than distinct digests: the walk stops at the tenth plan with one, and beyond it only the missed plans' rows are probed. */
            Assert.True(newProbes.Loops < Digests, $"the plan fetch probed query_plan_dim {newProbes.Loops} times for {Digests} distinct digests (the per-row statement probed {oldProbes.Loops} times)");
            Assert.False(newProbes.WholeScan, "the plan fetch scans query_plan_dim whole instead of probing it by digest");
        });

    /// <summary>
    /// A NULL cost sorts first, so ten plans whose only row has no cost and has a plan are the ten the per-row statement returns, in an order it leaves to ties.
    /// The walk finds them first, and no other plan can outrank a tenth hit with no cost, so the fetch probes no other plan's rows.
    /// </summary>
    [Fact]
    public Task TheFetch_ProbesNoOtherPlan_WhenTheTenthHitHasNoCost() =>
        WithSeedAsync(async (connection, start, end, ct) =>
        {
            /* Keys 2001..2010: one row each with no cost, on digests 13, 23, .. 103 (text). */
            await ExecAsync(connection, ct,
                InsertHead + @"SELECT $1 + 9000000 + k * 100, $2, $3, 'a8570', 'PlanDb', '0xH' || k, '0xP' || k, '0xS' || k, '0xL' || k,
                         NULL, NULL, sha256(convert_to('a8570p-d' || ((k - 2000) * 10 + 3), 'UTF8')),
                         10, NULL, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600
                  FROM generate_series(2001, 2010) k",
                CollectionIdGenerator.Next(), end.AddMinutes(-15), ServerId);
            await ExecAsync(connection, ct, "ANALYZE query_stats");

            var expected = await RowsAsync(connection, PerRowGuardSql, start, end, ct);
            var actual = await RowsAsync(connection, PgFactCollector.PlanAdvisorySql, start, end, ct);
            var costless = Enumerable.Range(1, 10).Select(n => $"<ShowPlanXML>plan {n * 10 + 3}</ShowPlanXML>|<null>").Order(StringComparer.Ordinal).ToList();
            Assert.Equal(costless, expected.Order(StringComparer.Ordinal).ToList());
            Assert.Equal(costless, actual.Order(StringComparer.Ordinal).ToList());

            var probes = await DimensionProbesAsync(connection, PgFactCollector.PlanAdvisorySql, start, end, ct);
            /* Ten probes find the ten hits and ten read their content; every other plan's rows would be thousands. */
            Assert.True(probes.Loops <= 30, $"the plan fetch probed query_plan_dim {probes.Loops} times when its ten hits have no cost");
            Assert.False(probes.WholeScan, "the plan fetch scans query_plan_dim whole instead of probing it by digest");
        });

    /// <summary>
    /// The rank reads the server's own rows. Its filter keeps the own plan column beside <c>query_plan_digest IS NOT NULL</c>: that predicate alone implies the
    /// partial index <c>ix_query_stats_digest_floor</c> (<c>collection_time</c> only), and the planner walks it: every server's rows in the window, each heap page
    /// fetched to test <c>server_id</c>. On a store of 18 servers that is 25,400 pages for one server's 86,000-row window, against 5,600 through the server index
    /// (#8570). The seed puts 40 other servers' rows into the same window. The statement must return the per-row statement's rows and never read query_stats
    /// through the partial index; the statement with the filter cut to the digest predicate must (so the seed is held to the defect).
    /// </summary>
    [Fact]
    public Task TheRank_ReadsTheServersOwnRows_NotEveryServersRowsThroughThePartialIndex() =>
        WithSeedAsync(async (connection, start, end, ct) =>
        {
            /* The collector appends snapshots in time order, so the heap follows collection_time, and the planner prices a walk of the time-only partial index
               as cheap for that reason. The seed's bulk rows are not in that order, so the table is rewritten in it. */
            await ExecAsync(connection, ct, "CREATE TEMP TABLE qs_ordered AS SELECT * FROM query_stats ORDER BY collection_time");
            await ExecAsync(connection, ct, "TRUNCATE query_stats");
            await ExecAsync(connection, ct, "INSERT INTO query_stats SELECT * FROM qs_ordered");
            await ExecAsync(connection, ct, "DROP TABLE qs_ordered");
            await ExecAsync(connection, ct, "ANALYZE query_stats");
            /* This seed is far smaller than a field window, where the planner never reads the table whole or through a bitmap. With both off the two indexes compete
               on cost alone, as they did on the field store. */
            await ExecAsync(connection, ct, "SET enable_seqscan = off");
            await ExecAsync(connection, ct, "SET enable_bitmapscan = off");
            /* The choice also turns on random_page_cost: a cluster tuned for SSDs (1.1, as timescaledb-tune sets it) prices the server index's scattered heap reads
               low enough that the server index wins whatever the filter, so the seed would not hold either statement to the trap. The session takes PostgreSQL's
               default. */
            await ExecAsync(connection, ct, "SET random_page_cost = 4");

            const string rankWhere = "AND   (f.query_plan_xml IS NOT NULL OR f.query_plan_digest IS NOT NULL)";
            Assert.Contains(rankWhere, PgFactCollector.PlanAdvisorySql);
            var withPredicate = PgFactCollector.PlanAdvisorySql.Replace(rankWhere, "AND   f.query_plan_digest IS NOT NULL");

            var expected = await RowsAsync(connection, PerRowGuardSql, start, end, ct);
            var actual = await RowsAsync(connection, PgFactCollector.PlanAdvisorySql, start, end, ct);
            Assert.Equal(10, actual.Count);
            Assert.Equal(expected, actual);

            Assert.True(await UsesPartialIndexAsync(connection, withPredicate, start, end, ct), "the seed does not put the planner on the partial index with the predicate, so it does not show the defect");
            Assert.False(await UsesPartialIndexAsync(connection, PgFactCollector.PlanAdvisorySql, start, end, ct), "the plan fetch reads query_stats through ix_query_stats_digest_floor, which walks every server's rows in the window");
        });

    private static async Task<bool> UsesPartialIndexAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + sql, connection) { CommandTimeout = 120 };
        AddParameters(command, start, end);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        if (Environment.GetEnvironmentVariable("PLAN_EXPLAIN_OUT") is { Length: > 0 } outPath)
        {
            await System.IO.File.AppendAllTextAsync(outPath, json + "\n", ct);
        }

        return json.Contains("ix_query_stats_digest_floor", StringComparison.Ordinal);
    }

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        AddParameters(command, start, end);
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var text = reader.IsDBNull(0) ? "<null>" : reader.GetString(0);
            var gz = reader.IsDBNull(1) ? "<null>" : Convert.ToHexString(reader.GetFieldValue<byte[]>(1));
            rows.Add(text + "|" + gz);
        }

        return rows;
    }

    /// <summary>The loops every query_plan_dim node of the statement's executed plan ran, and whether any of them read the whole table.</summary>
    private static async Task<(double Loops, bool WholeScan)> DimensionProbesAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection) { CommandTimeout = 120 };
        AddParameters(command, start, end);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        if (Environment.GetEnvironmentVariable("PLAN_EXPLAIN_OUT") is { Length: > 0 } outPath)
        {
            await System.IO.File.AppendAllTextAsync(outPath, json + "\n", ct);
        }

        using var document = JsonDocument.Parse(json);
        var loops = 0.0;
        var whole = false;
        Walk(document.RootElement[0].GetProperty("Plan"), ref loops, ref whole);
        return (loops, whole);
    }

    private static void Walk(JsonElement node, ref double loops, ref bool whole)
    {
        if (node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "query_plan_dim")
        {
            loops += node.GetProperty("Actual Loops").GetDouble();
            var type = node.GetProperty("Node Type").GetString();
            if (type is "Seq Scan" or "Parallel Seq Scan")
            {
                whole = true;
            }
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Walk(child, ref loops, ref whole);
            }
        }
    }

    private static void AddParameters(NpgsqlCommand command, DateTime start, DateTime end)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
    }

    /// <summary>
    /// 400 bulk keys of 50 snapshots over 200 digests, so a digest backs two keys and each key's rows share it. A digest by its number: no dimension row (a multiple
    /// of 10), a dimension row with no content (one past), gzip bytes only (two past), else text. Five special keys outrank the bulk and cover the guard's arms:
    /// 1001 has a costliest row whose digest is gone and cheaper rows on a digest with text (it must stand for itself through them), 1002 only gone digests,
    /// 1003 a gzip-only digest, 1004 a content-less digest, 1005 its own plan column with no digest. Key 1006 sits among the bulk keys: its costliest row's digest is gone
    /// and its cheaper row's digest has text and outranks the tenth bulk plan. 50,000 filler dimension rows give the planner a real primary key to choose against.
    /// </summary>
    private static async Task WithSeedAsync(Func<NpgsqlConnection, DateTime, DateTime, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live plan tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = scratch.ConnectionString;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var succeeded = false;
        try
        {
            await ExecAsync(connection, ct,
                @"INSERT INTO query_plan_dim (digest, query_plan_xml, query_plan_gz, last_seen)
                  SELECT sha256(convert_to('a8570p-d' || g, 'UTF8')),
                         CASE WHEN g % 10 >= 3 THEN '<ShowPlanXML>plan ' || g || '</ShowPlanXML>' END,
                         CASE WHEN g % 10 = 2 THEN decode(md5('gz' || g), 'hex') END,
                         now() AT TIME ZONE 'UTC'
                  FROM generate_series(1, 200) g
                  WHERE g % 10 <> 0");
            await ExecAsync(connection, ct,
                @"INSERT INTO query_plan_dim (digest, query_plan_xml, query_plan_gz, last_seen)
                  SELECT sha256(convert_to('a8570p-f' || g, 'UTF8')), '<ShowPlanXML>filler</ShowPlanXML>', NULL, now() AT TIME ZONE 'UTC'
                  FROM generate_series(1, 50000) g");
            await ExecAsync(connection, ct, "ANALYZE query_plan_dim");
            /* The field's dimension holds 52 million rows, far over any work_mem the planner would build a hash of, so it answers a per-row EXISTS with one index probe per row.
               A small seed would be hashed whole instead, so the connection gets the smallest work_mem to put the seed in the field's position. */
            await ExecAsync(connection, ct, "SET work_mem = '64kB'");

            var end = LiveClock.Now();
            var start = end.AddHours(-4);
            var id = CollectionIdGenerator.Next();
            /* Bulk: key g (1..400), snapshot r (0..49), digest ((g - 1) % 200) + 1. */
            await ExecAsync(connection, ct,
                InsertHead + @"SELECT $1 + g * 100 + r, $2 - (r * interval '4 minutes'), $3, 'a8570', 'PlanDb', '0xH' || g, '0xP' || g, '0xS' || g, '0xL' || g,
                         NULL, NULL, sha256(convert_to('a8570p-d' || (((g - 1) % 200) + 1), 'UTF8')),
                         10, 1000 + g * 100 + r, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600
                  FROM generate_series(1, 400) g CROSS JOIN generate_series(0, 49) r",
                id, end.AddMinutes(-10), ServerId);
            /* Special keys. Key 1001: row 0 is the costliest and its digest (10) has no dimension row; rows 1+ carry digest 3 (text). */
            await ExecAsync(connection, ct,
                InsertHead + @"SELECT $1 + 200000 + k * 100 + r, $2 - (r * interval '4 minutes'), $3, 'a8570', 'PlanDb', '0xH' || k, '0xP' || k, '0xS' || k, '0xL' || k,
                         NULL,
                         CASE WHEN k = 1005 THEN '<ShowPlanXML>own column plan</ShowPlanXML>' END,
                         CASE k WHEN 1001 THEN sha256(convert_to('a8570p-d' || CASE WHEN r = 0 THEN 10 ELSE 3 END, 'UTF8'))
                                WHEN 1002 THEN sha256(convert_to('a8570p-d20', 'UTF8'))
                                WHEN 1003 THEN sha256(convert_to('a8570p-d12', 'UTF8'))
                                WHEN 1004 THEN sha256(convert_to('a8570p-d11', 'UTF8'))
                                END,
                         10, (10000000 - (k - 1001) * 100000) - r * 1000, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600
                  FROM generate_series(1001, 1005) k CROSS JOIN generate_series(0, 9) r",
                id, end.AddMinutes(-10), ServerId);
            /* Key 1006: row 0 is the costliest (40,700, among the bulk keys' costliest rows) and its digest (10) has no dimension row; row 1 (40,500) carries digest 4 (text). */
            await ExecAsync(connection, ct,
                InsertHead + @"SELECT $1 + 200000 + 1006 * 100 + r, $2 - (r * interval '4 minutes'), $3, 'a8570', 'PlanDb', '0xH1006', '0xP1006', '0xS1006', '0xL1006',
                         NULL, NULL, sha256(convert_to('a8570p-d' || CASE WHEN r = 0 THEN 10 ELSE 4 END, 'UTF8')),
                         10, 40700 - r * 200, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600
                  FROM generate_series(0, 1) r",
                id, end.AddMinutes(-10), ServerId);
            /* Other servers' rows in the same window (#8570): each has a plan digest and no plan of its own, the rows the partial index covers. */
            await ExecAsync(connection, ct,
                InsertHead + @"SELECT $1 + 400000 + (s - 85701)::bigint * 100000 + g * 100 + r, $2 - (r * interval '4 minutes'), s, 'a8570o', 'PlanDb', '0xH' || g, '0xP' || g, '0xS' || g, '0xL' || g,
                         NULL, NULL, sha256(convert_to('a8570p-d' || (((g - 1) % 200) + 1), 'UTF8')),
                         10, 1000 + g * 100 + r, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600
                  FROM generate_series(85702, 85741) s CROSS JOIN generate_series(1, 200) g CROSS JOIN generate_series(0, 24) r",
                id, end.AddMinutes(-10));
            await ExecAsync(connection, ct, "ANALYZE query_stats");

            await body(connection, start, end.AddMinutes(5), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await ExecAsync(cleanupConnection, cleanupCt, "DELETE FROM query_stats WHERE server_id BETWEEN 85701 AND 85741"));
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        foreach (var value in parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
