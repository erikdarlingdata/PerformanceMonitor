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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9353: the PLAN_REGRESSION statements order by the CPU the regression lost and then the factor, and keep the first 100 (the fact) or the
/// first five (the drill-down). Rows that tie on both keys came out in no fixed order, so which 100 stayed could change from one read to the
/// next. The statements now end on the row's own key (database, query, replica role with a NULL role last), as Lite's do.
///
/// <para>Every query in each store costs the same, so every row ties on both keys. The rows are planted in an order that is not the key's, and
/// there are more of them than the cut keeps. The reads must give the same rows twice, and those rows are the first of the key's order.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. It CREATEs and DROPs its own databases through ScratchPostgres
   and then works entirely inside them. */
public sealed class PlanRegressionTieOrderLiveTests
{
    private const int ServerA = -9353101;
    private const int DrillServer = -9353102;

    /// <summary>Queries in the fact's store. Every fifth also has a PRIMARY group, so there are more than 100 rows and both role spellings.</summary>
    private const int Queries = 130;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Key(string database, long queryId, string role) => $"{database}|{queryId.ToString(CultureInfo.InvariantCulture)}|{role}";

    [Fact]
    public async Task TheFactsHundred_AreTheSameTwice_AndFollowTheRowKey_WhenRowsTieOnLostCpuAndFactor()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9353 plan regression tie order test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var (floor, built) = await SeedFactAsync(connection, ct);

        /* The key's order: database, query, then PRIMARY before the NULL role. Even queries are in "tiea", odd ones in "tieb". */
        var universe = new List<string>();
        for (long q = 1; q <= Queries; q++)
        {
            var database = q % 2 == 0 ? "tiea" : "tieb";
            universe.Add(Key(database, q, ""));
            if (q % 5 == 0)
            {
                universe.Add(Key(database, q, "PRIMARY"));
            }
        }

        Assert.True(universe.Count > 100);
        var expected = universe
            .Select(k => k.Split('|'))
            .OrderBy(p => p[0], StringComparer.Ordinal).ThenBy(p => long.Parse(p[1], CultureInfo.InvariantCulture))
            .ThenBy(p => p[2].Length == 0 ? 1 : 0)
            .Take(100)
            .Select(p => string.Join('|', p))
            .ToList();

        var first = await ReadFactAsync(connection, floor, built, ct);
        var second = await ReadFactAsync(connection, floor, built, ct);

        Assert.Equal(100, first.Count);
        Assert.Equal(first, second);
        Assert.Equal(expected, first);
    }

    [Fact]
    public async Task TheDrillDownsFive_AreTheSameTwice_AndFollowTheRowKey_WhenRowsTieOnLostCpuAndFactor()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9353 plan regression tie order test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var periodEnd = DateTime.SpecifyKind(new DateTime(LiveClock.Now().Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
        /* Seven rows, planted in the reverse of the key's order. */
        (string Database, long Query, string? Role)[] rows =
        [
            ("tieb", 3, null), ("tiea", 3, null), ("tieb", 2, null), ("tiea", 2, null), ("tieb", 1, null), ("tiea", 1, null), ("tiea", 1, "PRIMARY"),
        ];
        long plan = 1;
        foreach (var (database, query, role) in rows)
        {
            await SeedQueryStoreAsync(connection, database, query, role, plan++, 1_000, periodEnd.AddDays(-5), periodEnd, ct);
            await SeedQueryStoreAsync(connection, database, query, role, plan++, 100_000, periodEnd, periodEnd, ct);
        }

        var expected = new[]
        {
            Key("tiea", 1, "PRIMARY"), Key("tiea", 1, ""), Key("tiea", 2, ""), Key("tiea", 3, ""), Key("tieb", 1, ""),
        };

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        async Task<List<string>> ReadAsync()
        {
            var context = new AnalysisContext
            {
                ServerId = DrillServer,
                ServerName = "TieOrderDrill",
                TimeRangeStart = periodEnd.AddHours(-4),
                TimeRangeEnd = periodEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var finding = new AnalysisFinding { RootFactKey = "PLAN_REGRESSION", StoryPath = "PLAN_REGRESSION", PathKeys = ["PLAN_REGRESSION"], Severity = 1.0 };
            await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
            return ((IEnumerable<object>)finding.DrillDown!["regressed_queries"])
                .Select(i => JsonSerializer.SerializeToElement(i))
                .Select(e => Key(e.GetProperty("database").GetString()!, e.GetProperty("query_id").GetInt64(), e.GetProperty("replica_role").GetString()!))
                .ToList();
        }

        var first = await ReadAsync();
        var second = await ReadAsync();

        Assert.Equal(first, second);
        Assert.Equal(expected, first);
    }

    private static async Task<List<string>> ReadFactAsync(NpgsqlConnection connection, DateTime floor, List<PlanRegressionDaily.BuiltDay> built, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(PgFactCollector.PlanRegressionDailySql, connection) { CommandTimeout = 300 };
        command.Parameters.AddWithValue(ServerA);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Date, Value = built.Select(b => b.Day).ToArray() });
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, PgFactCollector.PlanRegressionLiveFloorWithWatermarks(floor, built));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, floor.AddDays(-1));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = built.Select(b => b.ThroughOrDayEnd).ToArray() });

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        var queryId = reader.GetOrdinal("query_id");
        var database = reader.GetOrdinal("database_name");
        var latestPlan = reader.GetOrdinal("latest_plan_id");
        while (await reader.ReadAsync(ct))
        {
            /* The plan ids say the role: q*100+1 and +2 are the NULL role's two plans, q*100+51 and +52 the PRIMARY role's. */
            var role = reader.GetInt64(latestPlan) % 100 > 50 ? "PRIMARY" : "";
            rows.Add(Key(reader.GetString(database), reader.GetInt64(queryId), role));
        }

        return rows;
    }

    /// <summary>
    /// Plants <see cref="Queries"/> queries in the daily totals: a best plan on three older days and a latest plan, fifty-fold the cost, on
    /// the last two days, the same for every query. Every fifth query has a second group under the PRIMARY role. The rows go in from the
    /// highest query down, so the table's own order is not the key's. Every day is built whole and the interval table is empty.
    /// </summary>
    private static async Task<(DateTime Floor, List<PlanRegressionDaily.BuiltDay> Built)> SeedFactAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var today = DateTime.SpecifyKind(LiveClock.Now().Date, DateTimeKind.Unspecified);
        var floor = PgFactCollector.PlanRegressionWindowFloor(today.AddDays(-14));

        await ExecAsync(connection, @"
INSERT INTO collect.plan_regression_daily
    (server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash, execs, cpu_us_sum, dur_us_sum, last_exec, is_forced_plan, force_failure_count)
SELECT @s, (@t::date - 7 + k), CASE WHEN q % 2 = 0 THEN 'tiea' ELSE 'tieb' END, q, q * 100 + v * 50 + 1, CASE WHEN v = 1 THEN 'PRIMARY' END, 'B' || q || '_' || v,
       100, 100 * 1000, 100 * 3000, (@t::date - 7 + k)::timestamp + interval '12 hours', false, 0
FROM generate_series(@r, 1, -1) AS q CROSS JOIN generate_series(0, 1) AS v CROSS JOIN generate_series(0, 2) AS k
WHERE v = 0 OR q % 5 = 0
UNION ALL
SELECT @s, (@t::date - 2 + k), CASE WHEN q % 2 = 0 THEN 'tiea' ELSE 'tieb' END, q, q * 100 + v * 50 + 2, CASE WHEN v = 1 THEN 'PRIMARY' END, 'L' || q || '_' || v,
       100, 100 * 100000, 100 * 300000, (@t::date - 2 + k)::timestamp + interval '12 hours', false, 0
FROM generate_series(@r, 1, -1) AS q CROSS JOIN generate_series(0, 1) AS v CROSS JOIN generate_series(0, 1) AS k
WHERE v = 0 OR q % 5 = 0",
            ("s", ServerA), ("t", today), ("r", Queries));
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

    private static async Task SeedQueryStoreAsync(
        NpgsqlConnection connection, string database, long query, string? role, long plan, long cpuUs, DateTime lastExec, DateTime collected, CancellationToken ct)
    {
        var firstExec = lastExec.AddHours(-1);
        await using var cmd = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     query_hash, query_plan_hash, execution_count,
     avg_cpu_time_us, avg_duration_us, is_forced_plan, force_failure_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, false, 0)", connection);
        cmd.Parameters.AddWithValue(CollectionIdGenerator.Next());
        cmd.Parameters.AddWithValue(collected.AddMinutes(-9));
        cmd.Parameters.AddWithValue(DrillServer);
        cmd.Parameters.AddWithValue("TieOrderDrill");
        cmd.Parameters.AddWithValue(database);
        cmd.Parameters.AddWithValue(query);
        cmd.Parameters.AddWithValue(plan);
        cmd.Parameters.AddWithValue(role is null ? DBNull.Value : role);
        cmd.Parameters.AddWithValue(plan);
        cmd.Parameters.AddWithValue(firstExec);
        cmd.Parameters.AddWithValue(firstExec);
        cmd.Parameters.AddWithValue(lastExec);
        cmd.Parameters.AddWithValue("0xQH" + database + query.ToString(CultureInfo.InvariantCulture));
        cmd.Parameters.AddWithValue("0xP" + plan.ToString(CultureInfo.InvariantCulture));
        cmd.Parameters.AddWithValue(500L);
        cmd.Parameters.AddWithValue(cpuUs);
        cmd.Parameters.AddWithValue(cpuUs);
        await cmd.ExecuteNonQueryAsync(ct);
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
