/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A6 lane LB-2: the live proof PR #4181 shipped without — that the three interval-honest
/// successor DAILIES (<c>query_stats_interval_daily</c>, <c>procedure_stats_interval_daily</c>,
/// <c>query_stats_db_interval_daily</c>) really are created by the ensure sweep on a real TimescaleDB,
/// idempotently, with a refresh policy and NO compression policy, and that they fill to match their
/// legacy siblings' totals once refreshed.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here goes through
   ScratchPostgres.CreateAsync, which reaches DARLING_TEST_PG only to CREATE and DROP its own database and then
   works entirely inside it. It never touches the shared database's tables, so it cannot race the live
   collection, and serializing it would be pure slowdown. Leave it out; this comment is here so the next sweep
   does not "fix" it. */
public sealed class SuccessorDailyLiveTests
{
    private const int ServerId = -936537;
    private const string ServerName = "successor-daily-e2e";

    /// <summary>Step 1: the ensure sweep creates all three successor dailies, attaches a refresh policy to
    /// each, attaches NO compression policy to any of them (the LC-freeze defer), and a SECOND sweep is a
    /// no-op — no error, no duplicate policy.</summary>
    [Fact]
    public async Task EnsureSweep_CreatesSuccessorDailies_WithRefreshButNoCompression_AndIsIdempotent()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live successor-daily ensure-sweep test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live successor-daily test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var successorDailies = new[]
        {
            TimescaleSupport.QueryStatsIntervalDailyView,
            TimescaleSupport.ProcedureStatsIntervalDailyView,
            TimescaleSupport.QueryStatsDbIntervalDailyView,
        };

        /* ── FIRST ensure: fresh store. ── */
        var log = new CapturingTestLogger();
        var readyFirst = await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, log, ct);
        Assert.Equal(
            TimescaleSupport.HourlyAggregates.Length + TimescaleSupport.DailyAggregates.Length
                + TimescaleSupport.BaselineAggregates.Length + TimescaleSupport.OffGridAggregates.Length,
            readyFirst);

        foreach (var view in successorDailies)
        {
            Assert.True(await RelationExistsAsync(connection, view, ct), $"{view} must exist after the ensure sweep: {log.Joined}");
            Assert.Equal(1, await RefreshPolicyCountAsync(connection, view, ct));
            Assert.Equal(0, await CompressionPolicyCountAsync(connection, view, ct));
        }

        /* ── SECOND ensure: idempotent — no error, no duplicate policy on any of the three. ── */
        var readySecond = await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        Assert.Equal(readyFirst, readySecond);

        foreach (var view in successorDailies)
        {
            Assert.True(await RelationExistsAsync(connection, view, ct), $"{view} must still exist after the second ensure sweep");
            Assert.Equal(1, await RefreshPolicyCountAsync(connection, view, ct));
            Assert.Equal(0, await CompressionPolicyCountAsync(connection, view, ct));
        }
    }

    /// <summary>Step 2: seed raw query_stats/procedure_stats over ~4 days with no restart, refresh the
    /// successor hourlies then the successor dailies, and assert each successor daily's per-day totals
    /// equal its legacy daily's (same keys, same sums; the successor's extra
    /// <c>sample_interval_seconds_sum</c> is ignored in the comparison).</summary>
    [Fact]
    public async Task SuccessorDailies_FillFromSuccessorHourlies_AndMatchLegacyDailyTotals()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live successor-daily fill/parity test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live successor-daily test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* Four whole UTC days, aligned to midnight so the daily buckets are unambiguous. Naive-UTC storage,
           as PgCollectorRowWriter writes it. */
        var startDay = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-5), DateTimeKind.Unspecified);
        var endDay = startDay.AddDays(4);

        await SeedQueryStatsAsync(connection, startDay, endDay, ct);
        await SeedProcedureStatsAsync(connection, startDay, endDay, ct);

        var ready = await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        Assert.Equal(
            TimescaleSupport.HourlyAggregates.Length + TimescaleSupport.DailyAggregates.Length
                + TimescaleSupport.BaselineAggregates.Length + TimescaleSupport.OffGridAggregates.Length,
            ready);

        /* Refresh the hourly tier FIRST — the dailies (legacy and successor alike) are hierarchical from an
           hourly, so refreshing a daily before its hourly source has materialized reads nothing new. */
        var hourlyViews = new[]
        {
            TimescaleSupport.QueryStatsHourlyView,
            TimescaleSupport.QueryStatsIntervalHourlyView,
            TimescaleSupport.QueryStatsDbHourlyView,
            TimescaleSupport.QueryStatsDbIntervalHourlyView,
            TimescaleSupport.ProcedureStatsHourlyView,
            TimescaleSupport.ProcedureStatsIntervalHourlyView,
        };
        foreach (var view in hourlyViews)
        {
            await RollupBackfill.RunSliceAsync(connection, view, startDay, endDay.AddDays(1), SilentDisclosure(), ct);
        }

        var dailyViews = new[]
        {
            TimescaleSupport.QueryStatsDailyView,
            TimescaleSupport.QueryStatsIntervalDailyView,
            TimescaleSupport.QueryStatsDbDailyView,
            TimescaleSupport.QueryStatsDbIntervalDailyView,
            TimescaleSupport.ProcedureStatsDailyView,
            TimescaleSupport.ProcedureStatsIntervalDailyView,
        };
        foreach (var view in dailyViews)
        {
            await RollupBackfill.RunSliceAsync(connection, view, startDay, endDay.AddDays(1), SilentDisclosure(), ct);
        }

        /* query_stats_daily vs query_stats_interval_daily: same keys, same sums per day. */
        await AssertDailyTotalsMatchAsync(
            connection,
            legacyView: TimescaleSupport.QueryStatsDailyView,
            successorView: TimescaleSupport.QueryStatsIntervalDailyView,
            keyColumns: "server_id, server_name, database_name, query_hash, sql_handle",
            sumColumns: new[] { "worker_time_sum", "elapsed_time_sum", "execution_count_sum" },
            ct);

        /* procedure_stats_daily vs procedure_stats_interval_daily. */
        await AssertDailyTotalsMatchAsync(
            connection,
            legacyView: TimescaleSupport.ProcedureStatsDailyView,
            successorView: TimescaleSupport.ProcedureStatsIntervalDailyView,
            keyColumns: "server_id, server_name, database_name, schema_name, object_name",
            sumColumns: new[] { "worker_time_sum", "elapsed_time_sum", "execution_count_sum" },
            ct);

        /* query_stats_db_daily vs query_stats_db_interval_daily. */
        await AssertDailyTotalsMatchAsync(
            connection,
            legacyView: TimescaleSupport.QueryStatsDbDailyView,
            successorView: TimescaleSupport.QueryStatsDbIntervalDailyView,
            keyColumns: "server_id, server_name, database_name",
            sumColumns: new[] { "worker_time_sum", "logical_reads_sum", "physical_reads_sum", "logical_writes_sum", "execution_count_sum" },
            ct);
    }

    /// <summary>Step 3: the runbook's verification query, run against a store after a
    /// <c>--backfill-rollups</c>-shaped slice-by-slice fill restricted to the three successor dailies (and
    /// their successor-hourly sources — the runbook's own dependency-order rule). Confirms
    /// <c>min(bucket)</c> of each successor daily is <c>&lt;= date_trunc('day', min(bucket))</c> of its
    /// successor hourly, the runbook's stated convergence condition.</summary>
    [Fact]
    public async Task RunbookVerificationQuery_HoldsAfterBackfillRestrictedToTheThreeSuccessorDailies()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live backfill-runbook verification test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live backfill-runbook test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var historyStart = now.AddDays(-6);

        await SeedQueryStatsAsync(connection, historyStart.Date, now.Date.AddDays(1), ct);
        await SeedProcedureStatsAsync(connection, historyStart.Date, now.Date.AddDays(1), ct);

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* Materialize only the successor HOURLIES first (their own RunSliceAsync, per the runbook's
           dependency order — the daily cannot hold more history than its source), then the three
           successor DAILIES named in the runbook — nothing else, mirroring the runbook's restriction. */
        var successorHourlies = new[]
        {
            TimescaleSupport.QueryStatsIntervalHourlyView,
            TimescaleSupport.ProcedureStatsIntervalHourlyView,
            TimescaleSupport.QueryStatsDbIntervalHourlyView,
        };
        foreach (var view in successorHourlies)
        {
            await RollupBackfill.RunSliceAsync(connection, view, historyStart.Date, now.Date.AddDays(1), SilentDisclosure(), ct);
        }

        var successorDailies = new[]
        {
            TimescaleSupport.QueryStatsIntervalDailyView,
            TimescaleSupport.ProcedureStatsIntervalDailyView,
            TimescaleSupport.QueryStatsDbIntervalDailyView,
        };
        foreach (var view in successorDailies)
        {
            await RollupBackfill.RunSliceAsync(connection, view, historyStart.Date, now.Date.AddDays(1), SilentDisclosure(), ct);
        }

        /* The runbook's own verification query, one pair at a time, exactly as written in
           docs/runbooks/a6-successor-daily-backfill.md. */
        foreach (var (dailyView, hourlyView) in new[]
        {
            (TimescaleSupport.QueryStatsIntervalDailyView, TimescaleSupport.QueryStatsIntervalHourlyView),
            (TimescaleSupport.ProcedureStatsIntervalDailyView, TimescaleSupport.ProcedureStatsIntervalHourlyView),
            (TimescaleSupport.QueryStatsDbIntervalDailyView, TimescaleSupport.QueryStatsDbIntervalHourlyView),
        })
        {
            await using var read = new NpgsqlCommand($@"
SELECT
    (SELECT min(bucket) FROM collect.{dailyView})  AS daily_floor,
    date_trunc('day', (SELECT min(bucket) FROM collect.{hourlyView})) AS hourly_day_floor", connection);
            await using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.False(reader.IsDBNull(0), $"{dailyView} must have materialized at least one bucket");
            Assert.False(reader.IsDBNull(1), $"{hourlyView} must have materialized at least one bucket");
            var dailyFloor = reader.GetDateTime(0);
            var hourlyDayFloor = reader.GetDateTime(1);
            Assert.True(dailyFloor <= hourlyDayFloor,
                $"{dailyView}'s floor ({dailyFloor:O}) must be <= date_trunc('day', {hourlyView}'s floor) ({hourlyDayFloor:O}) once the backfill has converged");
        }
    }

    private static async Task AssertDailyTotalsMatchAsync(
        NpgsqlConnection connection, string legacyView, string successorView, string keyColumns, string[] sumColumns, CancellationToken ct)
    {
        var sums = string.Join(", ", sumColumns.Select(c => $"sum({c}) AS {c}"));
        var legacySql = $@"SELECT {keyColumns}, time_bucket('1 day', bucket) AS day, {sums}
FROM collect.{legacyView}
GROUP BY {keyColumns}, day
ORDER BY {keyColumns}, day";
        var successorSql = $@"SELECT {keyColumns}, time_bucket('1 day', bucket) AS day, {sums}
FROM collect.{successorView}
GROUP BY {keyColumns}, day
ORDER BY {keyColumns}, day";

        var legacyRows = await ReadTotalsAsync(connection, legacySql, keyColumns, sumColumns, ct);
        var successorRows = await ReadTotalsAsync(connection, successorSql, keyColumns, sumColumns, ct);

        Assert.True(legacyRows.Count > 0, $"{legacyView} must have materialized rows for this comparison to mean anything");
        Assert.Equal(legacyRows.Count, successorRows.Count);

        for (var i = 0; i < legacyRows.Count; i++)
        {
            Assert.Equal(legacyRows[i].Key, successorRows[i].Key);
            Assert.Equal(legacyRows[i].Sums, successorRows[i].Sums);
        }
    }

    private sealed record TotalsRow(string Key, long[] Sums);

    private static async Task<System.Collections.Generic.List<TotalsRow>> ReadTotalsAsync(
        NpgsqlConnection connection, string sql, string keyColumns, string[] sumColumns, CancellationToken ct)
    {
        var keyCount = keyColumns.Split(',').Length;
        var rows = new System.Collections.Generic.List<TotalsRow>();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var keyParts = new string[keyCount + 1];
            for (var i = 0; i <= keyCount; i++)
            {
                keyParts[i] = reader.IsDBNull(i) ? "\u0000" : reader.GetValue(i).ToString() ?? "\u0000";
            }
            var key = string.Join("|", keyParts);

            var sums = new long[sumColumns.Length];
            for (var i = 0; i < sumColumns.Length; i++)
            {
                var ordinal = keyCount + 1 + i;
                sums[i] = reader.IsDBNull(ordinal) ? 0L : reader.GetInt64(ordinal);
            }

            rows.Add(new TotalsRow(key, sums));
        }
        return rows;
    }

    /// <summary><see cref="QueriesPerBucket"/> query_stats rows per hour across [from, to), no restart rows —
    /// each hour gets distinct query_hash values so the rollup materializes one row per (query, hour).</summary>
    private const int QueriesPerBucket = 6;

    private static async Task SeedQueryStatsAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    (extract(epoch FROM g)::bigint * 100 + q),
    g,
    $3,
    $5,
    'TestDb',
    decode(md5('sdq' || q::text), 'hex'),
    decode(md5('sdqh' || q::text), 'hex'),
    1000,
    2000,
    10,
    3600
FROM generate_series($1::timestamp, $2::timestamp, INTERVAL '1 hour') AS g
CROSS JOIN generate_series(1, $4::int) AS q", connection);

        insert.Parameters.AddWithValue(from);
        insert.Parameters.AddWithValue(to);
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(QueriesPerBucket);
        insert.Parameters.AddWithValue(ServerName);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task SeedProcedureStatsAsync(NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    (extract(epoch FROM g)::bigint * 100 + q + 1000000),
    g,
    $3,
    $5,
    'TestDb',
    'dbo',
    ('proc_' || q::text),
    decode(md5('sdph' || q::text), 'hex'),
    500,
    900,
    5,
    3600
FROM generate_series($1::timestamp, $2::timestamp, INTERVAL '1 hour') AS g
CROSS JOIN generate_series(1, $4::int) AS q", connection);

        insert.Parameters.AddWithValue(from);
        insert.Parameters.AddWithValue(to);
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(QueriesPerBucket);
        insert.Parameters.AddWithValue(ServerName);
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task<bool> RelationExistsAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        /* Continuous aggregates are NOT in pg_matviews (measured live: empty there); they resolve through
           to_regclass on their user-facing view name, the same probe RollupProbeSql uses. */
        await using var command = new NpgsqlCommand($"SELECT to_regclass('collect.{view}') IS NOT NULL", connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<long> RefreshPolicyCountAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT count(*)
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = $1", connection);
        command.Parameters.AddWithValue(view);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<long> CompressionPolicyCountAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT count(*)
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_compression'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = $1", connection);
        command.Parameters.AddWithValue(view);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static RefreshDisclosure SilentDisclosure() =>
        new(message => Assert.Fail($"the refresh degraded unexpectedly on a 2.28.1 store: {message}"));
}
