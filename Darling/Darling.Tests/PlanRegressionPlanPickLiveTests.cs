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
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: the two plan_ids a PLAN_REGRESSION row carries (the newest-run plan of the latest hash group and the most-executed plan
/// of the best hash group) are picked for the candidates only. They used to be ordered aggregates inside <c>plan_dedup</c>, which
/// sorted every hash group of the window on the plan_id tie-break and ran a per-group sort for the second one, to keep 100 rows
/// (113,000 hash groups on a busy primary).
///
/// <para>Two halves. One reads the executed plan of <see cref="PgFactCollector.PlanRegressionDailySql"/> and fails when a sort
/// keyed on <c>plan_id</c> sees more than the candidates' rows. The other reads the statement's output for queries built so the
/// tie-breaks decide (equal last run, equal executions) with a NULL and a non-NULL replica_role, and checks the picks.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It CREATEs and DROPs its own database through ScratchPostgres
   and then works entirely inside it. */
public sealed class PlanRegressionPlanPickLiveTests
{
    private const int ServerA = -8570101;

    /// <summary>Queries whose latest plan regressed. More than the statement's LIMIT of 100, so the pick runs on a cut list.</summary>
    private const int Regressed = 240;

    /// <summary>Single-plan queries that stay in the window with enough executions to survive the floor.</summary>
    private const int Filler = 3000;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ThePlanIdSorts_SeeTheCandidatesOnly_NotEveryHashGroupOfTheWindow()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8570 plan pick test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var (floor, built) = await SeedAsync(connection, ct);

        var root = await RunAsync(connection, floor, built, explain: true, ct);
        var sorts = new List<(string Keys, double Rows)>();
        CollectPlanIdSorts(root.RootElement[0].GetProperty("Plan"), sorts);

        /* The old text sorted every hash group (Regressed * 2 + Filler of them) on plan_id. The candidates are at most 100 rows
           and the two plans of each hash group, so any sort keyed on plan_id has well under a thousand rows. */
        var hashGroups = Regressed * 2 + Filler;
        Assert.True(hashGroups > 3000);
        Assert.All(sorts, s => Assert.True(s.Rows < 1000, $"a sort keyed on plan_id saw {s.Rows} rows of {hashGroups} hash groups: {s.Keys}"));
    }

    [Fact]
    public async Task TheLatestAndBestPlan_FollowTheTieBreaks_WithANullAndANonNullReplica()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8570 plan pick test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var (floor, built) = await SeedAsync(connection, ct);

        var rows = new List<(long QueryId, long Latest, long Best)>();
        await using (var command = BuildCommand(connection, floor, built, explain: false))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rows.Add((reader.GetInt64(0), reader.GetInt64(10), reader.GetInt64(11)));
            }
        }

        Assert.Equal(100, rows.Count);
        foreach (var (queryId, latest, best) in rows)
        {
            /* Even queries tie: the two plans of each hash group have the same last run and the same executions, so the
               higher plan_id wins both picks. Odd queries do not tie: the lower plan_id ran last in the latest group and
               has the most executions in the best group. */
            var baseId = queryId * 10;
            var odd = queryId % 2 == 1;
            Assert.Equal(odd ? baseId + 3 : baseId + 4, latest);
            Assert.Equal(odd ? baseId + 1 : baseId + 2, best);
        }

        /* Both replica spellings are in the 100 rows, so the NULL-safe match was exercised. */
        Assert.Contains(rows, r => r.QueryId % 3 == 0);
        Assert.Contains(rows, r => r.QueryId % 3 != 0);
    }

    private static NpgsqlCommand BuildCommand(NpgsqlConnection connection, DateTime floor, List<PlanRegressionDaily.BuiltDay> built, bool explain)
    {
        var command = new NpgsqlCommand((explain ? "EXPLAIN (ANALYZE, FORMAT JSON) " : string.Empty) + PgFactCollector.PlanRegressionDailySql, connection) { CommandTimeout = 300 };
        command.Parameters.AddWithValue(ServerA);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = built.Select(b => b.Day).ToArray() });
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, PgFactCollector.PlanRegressionLiveFloorWithWatermarks(floor, built));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor.AddDays(-1));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = built.Select(b => b.ThroughOrDayEnd).ToArray() });
        return command;
    }

    private static async Task<JsonDocument> RunAsync(NpgsqlConnection connection, DateTime floor, List<PlanRegressionDaily.BuiltDay> built, bool explain, CancellationToken ct)
    {
        /* One session, parallelism off so each node's row count is the whole count, and the collector's work_mem so the plan is the one it gets. */
        await using (var set = new NpgsqlCommand("SET max_parallel_workers_per_gather = 0; SET work_mem = '64MB'", connection))
        {
            await set.ExecuteNonQueryAsync(ct);
        }

        await using var command = BuildCommand(connection, floor, built, explain);
        return JsonDocument.Parse((string)(await command.ExecuteScalarAsync(ct))!);
    }

    private static void CollectPlanIdSorts(JsonElement node, List<(string Keys, double Rows)> found)
    {
        var type = node.GetProperty("Node Type").GetString();
        if ((type == "Sort" || type == "Incremental Sort") && node.TryGetProperty("Sort Key", out var keys))
        {
            var text = string.Join(", ", keys.EnumerateArray().Select(k => k.GetString()));
            if (text.Contains("plan_id DESC", StringComparison.Ordinal))
            {
                /* Actual Rows is per loop, so a sort that is only rescanned is not counted once per rescan. */
                found.Add((text, node.GetProperty("Actual Rows").GetDouble()));
            }
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                CollectPlanIdSorts(child, found);
            }
        }
    }

    /// <summary>
    /// Plants <see cref="Regressed"/> queries, each with a best hash group (two plan_ids, three older days) and a latest hash group
    /// (two plan_ids, the last two days, fifty times the CPU), plus <see cref="Filler"/> single-plan queries. Every day is built
    /// whole and the interval table is empty, so the read is the built days' totals only. Query q has plan_ids q*10+1 to q*10+4. A
    /// third of the queries carry a replica role, the rest are NULL.
    /// </summary>
    private static async Task<(DateTime Floor, List<PlanRegressionDaily.BuiltDay> Built)> SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var today = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
        var floor = PgFactCollector.PlanRegressionWindowFloor(today.AddDays(-14));

        await ExecAsync(connection, @"
INSERT INTO collect.plan_regression_daily
    (server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash, execs, cpu_us_sum, dur_us_sum, last_exec, is_forced_plan, force_failure_count)
-- the best group: plans +1 and +2, hash B, three older days. Odd queries: +1 ran more. Even queries: equal.
SELECT @s, (@t::date - 7 + k), 'planpick', q, q * 10 + p, CASE WHEN q % 3 = 0 THEN 'PRIMARY' END, 'B' || q,
       CASE WHEN q % 2 = 1 AND p = 1 THEN 150 ELSE 100 END,
       CASE WHEN q % 2 = 1 AND p = 1 THEN 150 ELSE 100 END * 1000,
       CASE WHEN q % 2 = 1 AND p = 1 THEN 150 ELSE 100 END * 3000,
       (@t::date - 7 + k)::timestamp + interval '12 hours', false, 0
FROM generate_series(1, @r) AS q CROSS JOIN generate_series(1, 2) AS p CROSS JOIN generate_series(0, 2) AS k
UNION ALL
-- the latest group: plans +3 and +4, hash L, the last two days, fifty times the cost. Odd queries: +3 ran last. Even: equal.
SELECT @s, (@t::date - 2 + k), 'planpick', q, q * 10 + 2 + p, CASE WHEN q % 3 = 0 THEN 'PRIMARY' END, 'L' || q,
       100, 100 * 50000, 100 * 150000,
       (@t::date - 2 + k)::timestamp + interval '12 hours' + CASE WHEN q % 2 = 1 AND p = 1 THEN interval '1 hour' ELSE interval '0' END, false, 0
FROM generate_series(1, @r) AS q CROSS JOIN generate_series(1, 2) AS p CROSS JOIN generate_series(0, 1) AS k
UNION ALL
-- filler: one plan per query, enough executions to pass the floor, nothing to compare against
SELECT @s, (@t::date - 7 + k), 'planpick', 100000 + q, 100000 + q, NULL, 'F' || q, 30, 30 * 1000, 30 * 3000,
       (@t::date - 7 + k)::timestamp + interval '12 hours', false, 0
FROM generate_series(1, @f) AS q CROSS JOIN generate_series(0, 1) AS k",
            ("s", ServerA), ("t", today), ("r", Regressed), ("f", Filler));
        await ExecAsync(connection, @"
INSERT INTO collect.plan_regression_daily_built (server_id, day, late_seq, built_seq, partial_seq, built_at, source_rows, built_through)
SELECT @s, d.d::date, 0, 0, NULL, d.d, 0, NULL
FROM generate_series(@f::timestamp, @t::timestamp - interval '1 day', interval '1 day') AS d (d)",
            ("s", ServerA), ("f", floor), ("t", today));
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES (@s, @f, @f)",
            ("s", ServerA), ("f", floor.AddDays(-5)));
        await ExecAsync(connection, "ANALYZE collect.plan_regression_daily");

        var built = new List<PlanRegressionDaily.BuiltDay>();
        await using (var days = new NpgsqlCommand(PlanRegressionDaily.BuiltDaysSql, connection))
        {
            days.Parameters.AddWithValue(ServerA);
            days.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor);
            await using var reader = await days.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                built.Add(new PlanRegressionDaily.BuiltDay(reader.GetFieldValue<DateOnly>(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1)));
            }
        }

        Assert.Equal(14, built.Count);
        return (floor, built);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        await command.ExecuteNonQueryAsync();
    }
}
