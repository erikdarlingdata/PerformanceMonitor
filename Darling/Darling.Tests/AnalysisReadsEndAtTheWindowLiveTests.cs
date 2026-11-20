/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: reads only the scratch database each test mints. */
/// <summary>
/// An analysis pass replayed for an earlier window reads the server as it was at that window's end. Its latest-capture reads (the
/// server settings, the max degree of parallelism the high-DOP count is held to, the server's version and properties, and the two
/// newest object snapshots the growth and contention anomalies compare) take the newest capture the window holds, and the plan
/// regression fact reads no Query Store snapshot stored after the end, so a row stored after the end does not change what the pass
/// says. Each read is run twice over the same rows, once ending at the window's end and once ending after the later row.
/// </summary>
[Collection("live-postgres")]
public sealed class AnalysisReadsEndAtTheWindowLiveTests
{
    private const int ServerId = -9471;
    private const string ServerName = "analysis-window-end";

    private static readonly DateTime s_before = new(2026, 3, 1, 8, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime s_inside = new(2026, 3, 1, 11, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime s_windowEnd = new(2026, 3, 1, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime s_after = new(2026, 3, 1, 16, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime s_laterEnd = new(2026, 3, 1, 17, 0, 0, DateTimeKind.Unspecified);

    private const string SkipReason =
        "Set DARLING_TEST_PG to a Postgres connection string to run the live analysis window-end tests (each mints its own scratch database).";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheServerSettings_AreTheCaptureAtTheWindowEnd_NotOneStoredAfterIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const string insert = @"
INSERT INTO collect.server_config
    (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, 'max degree of parallelism', $5, $5, TRUE, TRUE)";
        await ExecAsync(connection, ct, insert, 1L, s_windowEnd, ServerId, ServerName, 4L);
        await ExecAsync(connection, ct, insert, 2L, s_after, ServerId, ServerName, 8L);

        Assert.Equal(4L, await MaxDopAsync(connection, s_windowEnd, ct));
        Assert.Equal(8L, await MaxDopAsync(connection, s_after, ct));
    }

    [Fact]
    public async Task TheHighDopCount_IsHeldToTheMaxDopCapturedByTheWindowEnd_NotOneStoredAfterIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* One query ran at DOP 16 inside the window, while the server's max degree of parallelism was 16. The setting captured
           four hours after the window's end is 4, which a plan at DOP 16 cannot have run under. */
        const string config = @"
INSERT INTO collect.server_config
    (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, 'max degree of parallelism', $5, $5, TRUE, TRUE)";
        await ExecAsync(connection, ct, config, 1L, s_before, ServerId, ServerName, 16L);
        await ExecAsync(connection, ct, config, 2L, s_after, ServerId, ServerName, 4L);
        await ExecAsync(connection, ct, @"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, query_text, delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills,
     min_dop, max_dop, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time)
VALUES
    ($1, $2, $3, $4, 'db1', '0xHASH', '0xPLANHASH', '0xSQLH', '0xPLANH', 'SELECT 1', 10, 10000, 20000, 500, 0, 5, 100, 0, 1, 16, 800, 1500, 1600, 3000)",
            1L, s_inside, ServerId, ServerName);

        Assert.Equal(1L, await HighDopQueriesAsync(connection, s_windowEnd, ct));
        Assert.Equal(0L, await HighDopQueriesAsync(connection, s_laterEnd, ct));
    }

    [Fact]
    public async Task TheServerVersionAndProperties_AreTheCaptureAtTheWindowEnd_NotOneStoredAfterIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        const string insert = @"
INSERT INTO collect.server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition)
VALUES ($1, $2, $3, $4, 'Edition', $5, 'RTM', 3)";
        await ExecAsync(connection, ct, insert, 1L, s_windowEnd, ServerId, ServerName, "15.0.2000.5");
        await ExecAsync(connection, ct, insert, 2L, s_after, ServerId, ServerName, "16.0.1000.6");

        Assert.Equal(15, await ScalarAsync(connection, PgFactCollector.ServerMetadataSql, "major_version", s_windowEnd, ct));
        Assert.Equal(16, await ScalarAsync(connection, PgFactCollector.ServerMetadataSql, "major_version", s_after, ct));
        Assert.Equal("15.0.2000.5", await ScalarAsync(connection, PgFactCollector.ServerPropertiesSql, "product_version", s_windowEnd, ct));
        Assert.Equal("16.0.1000.6", await ScalarAsync(connection, PgFactCollector.ServerPropertiesSql, "product_version", s_after, ct));
    }

    [Fact]
    public async Task TheObjectAnomalies_CompareTheTwoNewestSnapshotsBeforeTheWindowEnd_NotOnesStoredAtOrAfterIt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* One object: 100 MB and 100 ms of row lock waits, then 150 MB and 900 ms inside the window, then 400 MB and 5,000 ms after it. */
        const string insert = @"
INSERT INTO collect.index_object_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, object_id, index_id,
     reserved_mb, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES ($1, $2, $3, $4, 'db1', 1, 101, 1, $5, $6, 0)";
        await ExecAsync(connection, ct, insert, 1L, s_before, ServerId, ServerName, 100m, 100L);
        await ExecAsync(connection, ct, insert, 2L, s_inside, ServerId, ServerName, 150m, 900L);
        await ExecAsync(connection, ct, insert, 3L, s_after, ServerId, ServerName, 400m, 5000L);

        Assert.Equal((100m, 150m), await GrowthAsync(connection, s_windowEnd, ct));
        Assert.Equal(800L, await ContentionAsync(connection, s_windowEnd, ct));
        /* The detector's window is open at its end: the snapshot stamped exactly on the end belongs to the next window. */
        Assert.Equal((100m, 150m), await GrowthAsync(connection, s_after, ct));
        Assert.Equal(800L, await ContentionAsync(connection, s_after, ct));
        Assert.Equal((150m, 400m), await GrowthAsync(connection, s_laterEnd, ct));
        Assert.Equal(4100L, await ContentionAsync(connection, s_laterEnd, ct));
    }

    [Theory]
    [InlineData("raw")]
    [InlineData("table")]
    [InlineData("days")]
    public async Task ThePlanRegressionFactAndItsDrillDown_OfAPassAnchoredAtAnInstant_ReadNoSnapshotStoredAfterIt(string route)
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* One query. Its cheap plan ran five days before the earlier window. Its costly plan (ten times the CPU) first ran an hour
           after the earlier window's end, inside the later window, and its snapshot was stored then. An old steady query keeps
           the store's floor below the read's bound. The raw route reads the collected snapshots; the other two read the
           interval table, which a coverage claim below the window makes the source, the last with the closed days' totals. */
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var laterEnd = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
        var earlierEnd = laterEnd.AddHours(-3);
        var earlierStart = earlierEnd.AddHours(-4);
        var raw = route == "raw";

        await ExecAsync(connection, ct, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late");
        await InsertIntervalAsync(connection, raw, queryId: 1, planId: 11, "0xCHEAP", cpuUs: 20_000, firstExec: earlierStart.AddDays(-5).AddHours(-1), ct);
        await InsertIntervalAsync(connection, raw, queryId: 1, planId: 12, "0xCOSTLY", cpuUs: 200_000, firstExec: earlierEnd.AddHours(1), ct);
        await InsertIntervalAsync(connection, raw, queryId: 2, planId: 21, "0xSTEADY", cpuUs: 2000, firstExec: earlierStart.AddDays(-16), ct);
        await ExecAsync(connection, ct, "ALTER TABLE collect.query_store_interval_latest ENABLE TRIGGER trg_plan_regression_daily_late");
        if (!raw)
        {
            await ExecAsync(connection, ct,
                "INSERT INTO collect.query_store_interval_latest_coverage (server_id, filled_since, applied_through) VALUES ($1, $2, $2) "
                + "ON CONFLICT (server_id) DO UPDATE SET filled_since = EXCLUDED.filled_since",
                ServerId, now.Date.AddDays(-40));
        }

        if (route == "days")
        {
            for (var d = -PgFactCollector.PlanRegressionWindowDays; d <= -3; d++)
            {
                await PlanRegressionDaily.BuildDayAsync(connection, ServerId, DateOnly.FromDateTime(earlierStart.Date.AddDays(d)), now, ct);
            }
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        AnalysisContext AnchoredAt(DateTime end) => new()
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = end.AddHours(-4),
            TimeRangeEnd = end,
            ServerUtcOffset = TimeSpan.Zero,
            AsOfUtc = end,
        };

        /* The fact of a pass anchored at the end, and the rows of the drill-down a caller runs for that pass. */
        async Task<(Fact? Fact, int DrillDownRows)> ReadAsync(DateTime end)
        {
            var pass = AnchoredAt(end);
            var fact = (await new PgFactCollector(postgres).CollectFactsAsync(pass)).SingleOrDefault(f => f.Key == "PLAN_REGRESSION");
            Assert.Equal(!raw, pass.PlanRegressionReadsIntervalTable);
            Assert.Equal(route == "days", pass.PlanRegressionWindowStart is not null);

            var finding = new AnalysisFinding { RootFactKey = "PLAN_REGRESSION", StoryPath = "PLAN_REGRESSION", PathKeys = ["PLAN_REGRESSION"], Severity = 1.0 };
            await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], pass);
            var rows = finding.DrillDown is not null && finding.DrillDown.TryGetValue("regressed_queries", out var listed)
                ? System.Text.Json.JsonSerializer.SerializeToElement(listed).GetArrayLength()
                : 0;
            return (fact, rows);
        }

        /* At the earlier window's end the store held the cheap plan only: nothing regressed, and nothing is listed. */
        var earlier = await ReadAsync(earlierEnd);
        Assert.Null(earlier.Fact);
        Assert.Equal(0, earlier.DrillDownRows);

        /* The later window's end is after the costly plan's snapshot, so the same rows are a regression there. */
        var later = await ReadAsync(laterEnd);
        Assert.NotNull(later.Fact);
        Assert.Equal(1, later.Fact!.Metadata["offender_count"]);
        Assert.Equal(1, later.DrillDownRows);
    }

    /// <summary>One Query Store interval of a plan, 100 runs, first run at <paramref name="firstExec"/>, last run half an hour later, stored five minutes after that: a collected snapshot (<paramref name="raw"/>) or the interval table's row.</summary>
    private static async Task InsertIntervalAsync(
        NpgsqlConnection connection, bool raw, long queryId, long planId, string planHash, long cpuUs, DateTime firstExec, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(raw
            ? @"
INSERT INTO collect.query_store_stats
(
    collection_id, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, runtime_stats_interval_id,
    first_execution_time, interval_start_time_utc, collection_time, query_plan_hash, query_hash, execution_count,
    avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan
)
VALUES ($2, $1, 'analysis-window-end', 'windowenddb', $2, $3, 'Regular', $3, $4, $4, $5, $6, $7, 100, $8, $9, $10, false)"
            : @"
INSERT INTO collect.query_store_interval_latest
(
    server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time,
    collection_time, query_plan_hash, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
    last_execution_time, is_forced_plan, force_failure_count
)
VALUES ($1, 'windowenddb', $2, $3, NULL, $3, $4, $5, $6, $7, 100, $8, $9, $10, false, 0)", connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(queryId);
        cmd.Parameters.AddWithValue(planId);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, firstExec);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, firstExec.AddMinutes(35));
        cmd.Parameters.AddWithValue(planHash);
        cmd.Parameters.AddWithValue("qh" + queryId);
        cmd.Parameters.AddWithValue(cpuUs);
        cmd.Parameters.AddWithValue(cpuUs * 3);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Timestamp, firstExec.AddMinutes(30));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] values)
    {
        using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        foreach (var value in values) cmd.Parameters.AddWithValue(value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long?> MaxDopAsync(NpgsqlConnection connection, DateTime windowEnd, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgFactCollector.ServerConfigSql, connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(windowEnd);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.GetString(0) == "max degree of parallelism")
            {
                return Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
            }
        }

        return null;
    }

    /// <summary>The high-DOP query count of the query statistics read, for a window from <see cref="s_before"/> to <paramref name="windowEnd"/>.</summary>
    private static async Task<long> HighDopQueriesAsync(NpgsqlConnection connection, DateTime windowEnd, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgFactCollector.QueryStatsSql, connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(s_before);
        cmd.Parameters.AddWithValue(windowEnd);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return Convert.ToInt64(reader.GetValue(reader.GetOrdinal("high_dop_queries")), CultureInfo.InvariantCulture);
    }

    /// <summary>The named column of the one row a latest-capture read answers for a window ending at <paramref name="windowEnd"/>.</summary>
    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, string column, DateTime windowEnd, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(windowEnd);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? reader.GetValue(reader.GetOrdinal(column)) : null;
    }

    /// <summary>The prior and current size of the object the growth anomaly reports, with both thresholds at zero.</summary>
    private static async Task<(decimal PriorMb, decimal CurrentMb)?> GrowthAsync(NpgsqlConnection connection, DateTime windowEnd, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgAnomalyDetector.ObjectGrowthSql, connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(0.0);
        cmd.Parameters.AddWithValue(0.0);
        cmd.Parameters.AddWithValue(windowEnd);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? (reader.GetFieldValue<decimal>(3), reader.GetFieldValue<decimal>(4)) : null;
    }

    /// <summary>The row lock wait the contention anomaly reports between the two snapshots, with its threshold at zero.</summary>
    private static async Task<long?> ContentionAsync(NpgsqlConnection connection, DateTime windowEnd, CancellationToken ct)
    {
        using var cmd = new NpgsqlCommand(PgAnomalyDetector.ObjectContentionSql, connection);
        cmd.Parameters.AddWithValue(ServerId);
        cmd.Parameters.AddWithValue(0.0);
        cmd.Parameters.AddWithValue(windowEnd);
        using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture) : null;
    }
}
