/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9111: the Query Store grid and its slicer take whole built UTC days from the closed-day rollups and the rest of the window from
/// the interval table. Each test seeds a store of 38 days with seven intervals a day for every plan (so a rollup row stands for
/// about seven interval rows), reads a set of windows from the interval table alone, builds the days, and reads the same windows
/// again: every grid column and every slicer value must be the same text, digit for digit.
/// </summary>
[Collection(ViewerLoggerStaticsCollection.Name)]
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreClosedDayRollupLiveTests : IDisposable
{
    internal const int ServerId = -9111283;
    private const int Top = 50;

    /// <summary>The seed helper's own anchor; the bulk rows run from 30 days before it to 8 days after it.</summary>
    private static readonly DateTime Anchor = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime DataStart = Anchor.AddDays(-30);
    internal static readonly DateTime DataEnd = DataStart.AddDays(38);

    /// <summary>The tick's clock: two days after the last seeded day, so every seeded day is closed.</summary>
    internal static readonly DateTime TickNow = DataEnd.AddDays(2);

    internal const int KeepEverything = 400;

    internal static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The windows every comparison reads: label, start, end, database filter.</summary>
    private static readonly (string Label, DateTime Start, DateTime End, string[]? Databases)[] Windows =
    [
        ("past day", DataEnd.AddDays(-1), DataEnd, null),
        ("past week", DataEnd.AddDays(-7), DataEnd, null),
        ("past 30 days", DataEnd.AddDays(-30), DataEnd, null),
        ("past 30 days, one database", DataEnd.AddDays(-30), DataEnd, new[] { "rollA" }),
        ("starts and ends inside a day", DataEnd.AddDays(-30).AddHours(13), DataEnd.AddDays(-2).AddHours(-5), null),
        ("starts on an interval's first instant", DataEnd.AddDays(-9).AddHours(23), DataEnd.AddDays(-1), null),
        /* The end edge alone: a whole-day start and an end inside a day (partial last day), and an end on an interval's first instant
           (the interval starting there is outside [start, end)). Both are read through the grid, whose end is the window's own. */
        ("ends inside a day", DataEnd.AddDays(-30), DataEnd.AddDays(-3).AddHours(5), null),
        ("ends on an interval's first instant", DataEnd.AddDays(-30), DataEnd.AddDays(-3).AddHours(7), null),
    ];

    private const string SeedSql = @"
INSERT INTO collect.query_store_interval_wide
(
    collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
    module_name, query_hash, execution_count,
    avg_duration_us, min_duration_us, max_duration_us, avg_cpu_time_us, min_cpu_time_us, max_cpu_time_us,
    avg_logical_io_reads, min_logical_io_reads, max_logical_io_reads, avg_logical_io_writes, min_logical_io_writes, max_logical_io_writes,
    avg_physical_io_reads, min_physical_io_reads, max_physical_io_reads, avg_clr_time_us, min_clr_time_us, max_clr_time_us,
    min_dop, max_dop, avg_query_max_used_memory, min_query_max_used_memory, max_query_max_used_memory,
    avg_rowcount, min_rowcount, max_rowcount, avg_num_physical_io_reads, min_num_physical_io_reads, max_num_physical_io_reads,
    avg_log_bytes_used, min_log_bytes_used, max_log_bytes_used, avg_tempdb_space_used, min_tempdb_space_used, max_tempdb_space_used,
    plan_type, plan_forcing_type, is_forced_plan, force_failure_count, last_force_failure_reason, compatibility_level,
    query_plan_hash, replica_role, runtime_stats_interval_id, interval_start_time_utc, interval_end_time_utc
)
SELECT
    i.starts + interval '70 minutes', @server_id, CASE WHEN q % 2 = 0 THEN 'rollA' ELSE 'rollB' END, 1000 + q, 2000 + q,
    CASE WHEN q % 6 = 0 THEN 'Aborted' ELSE 'Regular' END, i.starts + interval '10 minutes', i.starts + interval '65 minutes',
    CASE WHEN q % 4 = 0 THEN 'dbo.proc_' || q END, '0xH' || q, 1 + (q * 7 + d * 3 + h) % 50,
    v.n, v.n / 2, v.n * 3, v.n + 11, (v.n + 11) / 2, (v.n + 11) * 3,
    v.n % 977, (v.n % 977) / 2, (v.n % 977) * 3, v.n % 53, 0, (v.n % 53) * 2,
    CASE WHEN q % 7 = 0 THEN NULL ELSE v.n % 31 END, CASE WHEN q % 7 = 0 THEN NULL ELSE 0 END, CASE WHEN q % 7 = 0 THEN NULL ELSE (v.n % 31) * 4 END,
    CASE WHEN q % 5 = 0 THEN NULL ELSE v.n % 13 END, CASE WHEN q % 5 = 0 THEN NULL ELSE 0 END, CASE WHEN q % 5 = 0 THEN NULL ELSE v.n % 17 END,
    1 + d % 2, 1 + (q + d) % 8, v.n % 4096, (v.n % 4096) / 2, (v.n % 4096) * 2,
    v.n % 211, 0, (v.n % 211) * 5, v.n % 19, 0, (v.n % 19) * 3,
    v.n % 7001, 0, (v.n % 7001) * 2, CASE WHEN h = 7 THEN NULL ELSE v.n % 89 END, 0, (v.n % 89) * 2,
    CASE WHEN q % 3 = 0 THEN 'Compiled Plan' END, CASE WHEN q = 3 AND d = 10 THEN 'MANUAL' END, q = 3 AND d = 10, (q + d) % 3,
    CASE WHEN (q + d) % 11 = 0 THEN 'NO_PLAN' END, 150 + (d % 2) * 10,
    '0xP' || q || '_' || (d % 3), CASE WHEN q % 9 = 0 THEN 'Secondary' END, d * 24 + h, i.starts, i.starts + interval '1 hour'
FROM generate_series(0, 37) AS d
CROSS JOIN unnest(ARRAY[0, 3, 7, 11, 15, 19, 23]) AS h
CROSS JOIN generate_series(1, 60) AS q
CROSS JOIN LATERAL (SELECT @data_start + make_interval(days => d, hours => h) AS starts) AS i
CROSS JOIN LATERAL (SELECT CAST(1000 + (q * 131 + d * 17 + h * 7) % 90000 AS bigint) AS n) AS v;";

    internal sealed class Store : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public required DarlingCollectorRunner Runner { get; init; }
        public required ViewerDataService Viewer { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Viewer.DisposeAsync();
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    internal static async Task<Store> SeedAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        /* The collector's own writer registers the server, its coverage and the raw rows the read gate looks at. */
        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(runner, ServerId, Anchor, ct);
        await ExecuteAsync(connection, "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", ct,
            ("server_id", ServerId), ("value", Anchor.AddDays(-90)));
        await ExecuteAsync(connection, SeedSql, ct, ("server_id", ServerId), ("data_start", DataStart));
        /* The tick plans enabled servers of the registry; the seed helper writes rows without registering one. */
        await ExecuteAsync(connection, "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (@server_id, 'rollup-9111', TRUE) ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct,
            ("server_id", ServerId));
        /* The seed helper's rows went through the writer after their days closed, which marks those days; the tests start from a
           store with no built row at all. */
        await ExecuteAsync(connection, "DELETE FROM collect.query_store_closed_day_built", ct);

        return new Store
        {
            Scratch = scratch,
            Connection = connection,
            Postgres = postgres,
            Runner = runner,
            Viewer = new ViewerDataService(scratch.ConnectionString),
        };
    }

    internal static async Task<int> ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        return await command.ExecuteNonQueryAsync(ct);
    }

    internal static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>One grid row as text: every column the grid statement returns, each number in its shortest round-trip form, so a
    /// value that differs in any digit is a different line.</summary>
    private static string GridLine(ViewerQueryStoreRow r) => JsonSerializer.Serialize(new object?[]
        {
            r.DatabaseName, r.QueryId, r.PlanId, r.ReplicaRole, r.QueryHash, r.QueryText,
            r.ModuleName, r.TotalExecutions, r.AvgDurationMs, r.AvgCpuTimeMs, r.AvgLogicalReads, r.AvgLogicalWrites,
            r.AvgPhysicalReads, r.AvgRowcount, r.MinDop, r.MaxDop, r.LastExecutionTime, r.QueryPlanHash,
            r.IsForcedPlan, r.PlanForcingType, r.ExecutionTypeDesc, r.FirstExecutionTime, r.AvgClrTimeMs, r.AvgTempdbSpaceUsed,
            r.AvgLogBytesUsed, r.PlanType, r.ForceFailureCount, r.LastForceFailureReason, r.CompatibilityLevel, r.MinDurationMs,
            r.MaxDurationMs, r.MinCpuTimeMs, r.MaxCpuTimeMs, r.MinLogicalReads, r.MaxLogicalReads, r.MinLogicalWrites,
            r.MaxLogicalWrites, r.MinPhysicalReads, r.MaxPhysicalReads, r.MinClrTimeMs, r.MaxClrTimeMs, r.MinRowcount,
            r.MaxRowcount, r.MinLogBytesUsed, r.MaxLogBytesUsed, r.MinTempdbSpaceUsed, r.MaxTempdbSpaceUsed, r.AvgMemoryMb,
            r.MinMemoryMb, r.MaxMemoryMb, r.AvgNumPhysicalIoReads, r.MinNumPhysicalIoReads, r.MaxNumPhysicalIoReads,
        });

    /// <summary>Every window's grid page (every column of every row) and slicer points, as text.</summary>
    private static async Task<Dictionary<string, List<string>>> ReadAllAsync(ViewerDataService viewer, CancellationToken ct)
    {
        var all = new Dictionary<string, List<string>>();
        foreach (var (label, start, end, databases) in Windows)
        {
            /* An open end, as the Viewer's presets pass: the gate serves a literal end only at or after the last applied batch. */
            var grid = await viewer.GetQueryStoreTopQueriesWithReachAsync(ServerId, start, end, Top, databases, literalEndUtc: null, tags: null, ct);
            Assert.True(grid.Plan is { UseTable: true }, $"{label}: the grid read did not take the interval-table route, so the comparison would prove nothing");
            Assert.True(grid.Rows.Count > 0, $"{label}: the grid returned no rows");
            all[label + " / grid"] = grid.Rows.Select(GridLine).ToList();

            var slicer = await viewer.GetQueryStoreSlicerDataAsync(ServerId, start, end, databases, ct);
            /* A window under the slicer's seven days reads the raw rows, which this seed does not fill. */
            Assert.True(slicer.Count > 0 || end - start < QueryStoreIntervalWide.SlicerWideMinWindow, $"{label}: the slicer returned no points");
            all[label + " / slicer"] = slicer.Select(b => JsonSerializer.Serialize(new object?[] { b.BucketTime, b.SessionCount, b.TotalCpu, b.TotalElapsed, b.TotalReads, b.TotalWrites, b.TotalPhysicalReads })).ToList();
        }

        return all;
    }

    private static void AssertSame(Dictionary<string, List<string>> expected, Dictionary<string, List<string>> actual, string stage)
    {
        foreach (var (label, lines) in expected)
        {
            Assert.True(lines.SequenceEqual(actual[label]), $"{stage}: {label} differs from the interval-table read");
        }
    }

    /// <summary>Builds every due day at the tick's clock, then stamps the builds and the heartbeat at the store's own clock, which is what
    /// the reader's freshness tests compare with.</summary>
    internal static async Task<QueryStoreClosedDayRollup.TickResult> TickAsync(Store store, int retentionDays, CancellationToken ct, DateTime? now = null, int? rawRetentionDays = null, bool allowStepFailure = false)
    {
        QueryStoreClosedDayRollup.SetReadClockForTests(store.Connection.Database, now ?? TickNow, retentionDays);
        var result = await QueryStoreClosedDayRollup.RunTickAsync(store.Postgres, now ?? TickNow, retentionDays, rawRetentionDays, NullLogger.Instance, ct);
        Assert.Equal(0, result.Failed);
        if (!allowStepFailure)
        {
            Assert.Equal(0, result.HoursFailed);
            Assert.Equal(0, result.ClutterDaysFailed);
        }
        await ExecuteAsync(store.Connection, "UPDATE collect.query_store_closed_day_built SET built_at = now() AT TIME ZONE 'UTC', clutter_built_at = CASE WHEN clutter_built_at = built_at THEN now() AT TIME ZONE 'UTC' END WHERE NOT removed", ct);
        await ExecuteAsync(store.Connection, "UPDATE collect.query_store_closed_day_tick SET ticked_at = (now() AT TIME ZONE 'UTC') - interval '10 minutes'", ct);
        if (rawRetentionDays is not null)
        {
            /* #9173: the hour heartbeat, on the store's clock like the day one. */
            await ExecuteAsync(store.Connection, "UPDATE collect.query_store_closed_day_tick SET hour_ticked_at = (now() AT TIME ZONE 'UTC') - interval '10 minutes'", ct);
        }

        return result;
    }

    private static Task<QueryStoreClosedDayRollup.ServedDays?> ServedAsync(Store store, DateTime start, DateTime end, CancellationToken ct) =>
        QueryStoreClosedDayRollup.ResolveServedDaysAsync(store.Connection, null, ServerId, start, end, 30, ct);

    /// <summary>
    /// The rung sits on a dense ladder, makes its four tables empty, runs twice, and the store probe still reads the migrated store as the
    /// newest version. It does not assert that the rung is the newest: later rungs land on top of it, and a newer rung's sentinel can
    /// answer for a store that lacks this rung's tables, so the probe's own V283 arm is proved by the ladder's pure probe tests.
    /// </summary>
    [Fact]
    public async Task TheRungIsOnADenseLadder_MakesItsTablesEmpty_RunsTwice_AndTheStoreProbeSeesThem()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == QueryStoreClosedDayRollup.Version);
        Assert.Contains(PgMigrations.Scripts, s => s.Version == rung.Version - 1);

        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9111 closed-day rollup live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        var tables = new[] { "query_store_grid_daily", "query_store_slicer_hourly", "query_store_closed_day_built", "query_store_closed_day_tick" };
        foreach (var table in tables)
        {
            await ExecuteAsync(connection, $"DROP TABLE IF EXISTS collect.{table}", ct);
        }
        await ExecuteAsync(connection, rung.Sql, ct);
        await ExecuteAsync(connection, rung.Sql, ct);
        foreach (var table in tables)
        {
            Assert.Equal(0L, await ScalarAsync<long>(connection, $"SELECT count(*) FROM collect.{table}", ct));
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        Assert.Equal(StorageVersion.SchemaVersion, await viewer.GetStoreSchemaVersionAsync(ct));
    }

    /// <summary>#9287: the read result the slow-load line and the grid banner are built from names the UTC days a read took from the
    /// rollups and the days it read from the interval table. Asserted on the day lists, never on time.</summary>
    [Fact]
    public async Task TheReadsSource_NamesTheRollupDaysAndTheIntervalRemainder_AndWhyWhenNoneWereTaken()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9287 read source live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);
        var start = DataEnd.AddDays(-30);

        async Task<(QueryStoreIntervalWide.WideReadPlan Plan, QueryStoreIntervalWide.WideReadPlan SlicerPlan)> ReadAsync()
        {
            var grid = await store.Viewer.GetQueryStoreTopQueriesWithReachAsync(ServerId, start, DataEnd, Top, null, literalEndUtc: null, tags: null, ct);
            var slicer = await store.Viewer.GetQueryStoreSlicerWithReachAsync(ServerId, start, DataEnd, null, ct);
            Assert.True(grid.Plan is { UseTable: true, Source: not null }, "the grid read did not take the interval-table route");
            Assert.True(slicer.Plan is { UseTable: true, Source: not null }, "the slicer read did not take the interval-table route");
            return (grid.Plan!.Value, slicer.Plan!.Value);
        }

        /* Nothing is built: every day is read from the interval table, and the source says the days are not built. */
        var (plan, slicerPlan) = await ReadAsync();
        var touched = DateOnly.FromDateTime(DataEnd).DayNumber - DateOnly.FromDateTime(plan.ReadStart).DayNumber;
        foreach (var p in new[] { plan, slicerPlan })
        {
            Assert.Empty(p.Source!.RollupDays);
            Assert.Equal(touched, p.Source.IntervalDays.Count);
            Assert.Equal(QueryStoreClosedDayRollup.SourceReason.NoBuiltDays, p.Source.Reason);
        }

        /* Every day built: no closed day is read as interval rows. The only interval day left is the window's partial first day. */
        await TickAsync(store, KeepEverything, ct);
        (plan, slicerPlan) = await ReadAsync();
        foreach (var p in new[] { plan, slicerPlan })
        {
            Assert.All(p.Source!.IntervalDays, d => Assert.Equal(DateOnly.FromDateTime(p.ReadStart), d));
            Assert.True(p.Source.RollupDays.Count >= touched - 1, $"{p.Source.RollupDays.Count} rollup days of {touched}");
            Assert.Equal(QueryStoreClosedDayRollup.SourceReason.Served, p.Source.Reason);
        }

        /* One built day and the rest unbuilt: one rollup day, and the interval table for the remainder. */
        var built = DateOnly.FromDateTime(DataEnd.AddDays(-12));
        await ExecuteAsync(store.Connection, "DELETE FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc <> @day", ct,
            ("server_id", ServerId), ("day", built));
        (plan, slicerPlan) = await ReadAsync();
        foreach (var p in new[] { plan, slicerPlan })
        {
            Assert.Equal(new[] { built }, p.Source!.RollupDays);
            Assert.Equal(touched - 1, p.Source.IntervalDays.Count);
            Assert.DoesNotContain(built, p.Source.IntervalDays);
            Assert.Contains("1 day from rollups", p.Source.Describe(), StringComparison.Ordinal);
        }

        /* Built days with a heartbeat older than two hours: no rollup day is taken, and the source says why. */
        await TickAsync(store, KeepEverything, ct);
        await ExecuteAsync(store.Connection, "UPDATE collect.query_store_closed_day_tick SET ticked_at = (now() AT TIME ZONE 'UTC') - interval '3 hours'", ct);
        (plan, slicerPlan) = await ReadAsync();
        foreach (var p in new[] { plan, slicerPlan })
        {
            Assert.Empty(p.Source!.RollupDays);
            Assert.Equal(touched, p.Source.IntervalDays.Count);
            Assert.Equal(QueryStoreClosedDayRollup.SourceReason.HeartbeatStale, p.Source.Reason);
            Assert.NotNull(p.Source.BannerNote());
        }
    }

    [Fact]
    public async Task BuiltDaysPlusEdges_EqualTheIntervalTable_ThroughALateRow_AnUnbuiltDay_AndStoppedBuilds()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9111 closed-day rollup live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);

        /* Nothing is built: every window is read from the interval table. This is the answer every later read must give. */
        Assert.Null(await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));
        var expected = await ReadAllAsync(store.Viewer, ct);

        var tick = await TickAsync(store, KeepEverything, ct);
        Assert.True(tick.Built >= 38, $"the tick built {tick.Built} day(s); all 38 seeded days are closed");
        var intervalRows = await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_interval_wide WHERE server_id = @server_id AND interval_start_time_utc >= @from AND interval_start_time_utc < @to", ct,
            ("server_id", ServerId), ("from", DataStart), ("to", DataEnd));
        var dailyRows = await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_grid_daily WHERE server_id = @server_id AND day_utc >= @from AND day_utc < @to", ct,
            ("server_id", ServerId), ("from", DateOnly.FromDateTime(DataStart)), ("to", DateOnly.FromDateTime(DataEnd)));
        Assert.True(intervalRows >= dailyRows * 6, $"{intervalRows} interval rows over {dailyRows} daily rows: the seed should hold about seven per daily row");

        /* The 30-day window now takes its 30 whole days from the rollup, and the answers are the same text. */
        var served = await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct);
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(DataEnd.AddDays(-30), DataEnd), served);
        AssertSame(expected, await ReadAllAsync(store.Viewer, ct), "after the build");

        /* The rollups are what answer: a change made to a rollup alone shows in the grid and in the slicer. */
        var probeDay = DateOnly.FromDateTime(DataEnd.AddDays(-12));
        await ExecuteAsync(store.Connection,
            "UPDATE collect.query_store_grid_daily SET execution_count = execution_count + 1000000 WHERE server_id = @server_id AND day_utc = @day", ct,
            ("server_id", ServerId), ("day", probeDay));
        await ExecuteAsync(store.Connection,
            "UPDATE collect.query_store_slicer_hourly SET cpu_wsum = cpu_wsum + 1000000000 WHERE server_id = @server_id AND hour_utc >= @day AND hour_utc < @day + interval '1 day'", ct,
            ("server_id", ServerId), ("day", probeDay.ToDateTime(TimeOnly.MinValue)));
        var changed = await ReadAllAsync(store.Viewer, ct);
        Assert.False(expected["past 30 days / grid"].SequenceEqual(changed["past 30 days / grid"]), "the grid did not read the daily rollup");
        Assert.False(expected["past 30 days / slicer"].SequenceEqual(changed["past 30 days / slicer"]), "the slicer did not read the hourly rollup");

        /* A day with no built row is read from the interval table: the changed day's built row goes, and the answers are right again. */
        await ExecuteAsync(store.Connection, "DELETE FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc = @day", ct,
            ("server_id", ServerId), ("day", probeDay));
        var around = await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct);
        Assert.True(around is { } run && (run.End <= probeDay.ToDateTime(TimeOnly.MinValue) || run.Start > probeDay.ToDateTime(TimeOnly.MinValue)),
            "the unbuilt day is still inside the run the read takes from the rollups");
        AssertSame(expected, await ReadAllAsync(store.Viewer, ct), "with one closed day unbuilt");

        /* The next tick builds it again from the interval table, which puts the changed rollup rows right. */
        Assert.Equal(1, (await TickAsync(store, KeepEverything, ct)).Built);
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(DataEnd.AddDays(-30), DataEnd), await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));
        AssertSame(expected, await ReadAllAsync(store.Viewer, ct), "after the unbuilt day was built again");

        /* A late row: the collector's writer puts new rows into days that are built. Those days are marked in the same write. */
        var lateAnchor = Anchor.AddDays(-10);
        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(store.Runner, ServerId, lateAnchor, ct);
        await ExecuteAsync(store.Connection, "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", ct,
            ("server_id", ServerId), ("value", Anchor.AddDays(-90)));
        var marked = await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND rebuild_needed AND day_utc >= @from AND day_utc < @to", ct,
            ("server_id", ServerId), ("from", DateOnly.FromDateTime(DataEnd.AddDays(-30))), ("to", DateOnly.FromDateTime(DataEnd)));
        Assert.True(marked > 0, "the writer put rows into built days and marked none of them for a rebuild");

        /* What the interval table alone says now: with the builds made to look stopped, no day is taken from the rollups. */
        await ExecuteAsync(store.Connection, "UPDATE collect.query_store_closed_day_built SET built_at = built_at - interval '3 days'", ct);
        Assert.Null(await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));
        var expectedAfterLate = await ReadAllAsync(store.Viewer, ct);
        Assert.False(expected["past 30 days / slicer"].SequenceEqual(expectedAfterLate["past 30 days / slicer"]),
            "the late rows changed nothing in the 30-day window, so the rebuild would prove nothing");

        /* Marked days are left to the interval table until they are rebuilt. */
        await ExecuteAsync(store.Connection, "UPDATE collect.query_store_closed_day_built SET built_at = now() AT TIME ZONE 'UTC', clutter_built_at = CASE WHEN clutter_built_at = built_at THEN now() AT TIME ZONE 'UTC' END WHERE NOT removed", ct);
        Assert.NotNull(await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));
        AssertSame(expectedAfterLate, await ReadAllAsync(store.Viewer, ct), "with the late rows' days marked");

        /* The tick rebuilds them, nothing stays marked, and the whole window is served from the rollups again. */
        Assert.True((await TickAsync(store, KeepEverything, ct)).Built >= marked);
        Assert.Equal(0L, await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND rebuild_needed", ct, ("server_id", ServerId)));
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(DataEnd.AddDays(-30), DataEnd), await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));
        AssertSame(expectedAfterLate, await ReadAllAsync(store.Viewer, ct), "after the late rows' days were rebuilt");
    }

    [Fact]
    public async Task ALateRowAnOlderServiceWritesWithoutAMark_IsReadFromTheIntervalTableOnceTheHeartbeatAges_AndTheNextTickRebuildsItsDay()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9111 closed-day rollup live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);

        var expected = await ReadAllAsync(store.Viewer, ct);
        await TickAsync(store, KeepEverything, ct);
        var window = (Start: DataEnd.AddDays(-30), End: DataEnd);
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(window.Start, window.End), await ServedAsync(store, window.Start, window.End, ct));
        AssertSame(expected, await ReadAllAsync(store.Viewer, ct), "after the build");

        /* A late row written the old way: a plain INSERT into a built day, no mark. */
        var lateDay = DateOnly.FromDateTime(DataEnd.AddDays(-12));
        var lateCollected = TickNow.AddMinutes(5);
        await using (var tx = await store.Connection.BeginTransactionAsync(ct))
        {
            await using var copy = new NpgsqlCommand(@"
CREATE TEMP TABLE late_rows ON COMMIT DROP AS
SELECT * FROM collect.query_store_interval_wide
WHERE server_id = @server_id AND interval_start_time_utc >= @day AND interval_start_time_utc < @day + interval '1 day' ORDER BY avg_duration_us DESC LIMIT 40;
UPDATE late_rows SET runtime_stats_interval_id = runtime_stats_interval_id + 900000000, collection_time = @collected, execution_count = execution_count * 9;
INSERT INTO collect.query_store_interval_wide SELECT * FROM late_rows;", store.Connection, tx);
            copy.Parameters.AddWithValue("server_id", ServerId);
            copy.Parameters.AddWithValue("day", lateDay.ToDateTime(TimeOnly.MinValue));
            copy.Parameters.AddWithValue("collected", lateCollected);
            await copy.ExecuteNonQueryAsync(ct);
            await tx.CommitAsync(ct);
        }

        Assert.Equal(40L, await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_interval_wide WHERE server_id = @server_id AND collection_time = @collected", ct, ("server_id", ServerId), ("collected", lateCollected)));
        Assert.Equal(0L, await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND rebuild_needed", ct, ("server_id", ServerId)));

        /* The older service ticks nothing, so the heartbeat ages past its limit, and the server is read from the interval table. */
        var aged = TickNow - QueryStoreClosedDayRollup.HeartbeatMargin - TimeSpan.FromHours(3);
        await ExecuteAsync(store.Connection, "UPDATE collect.query_store_closed_day_tick SET ticked_at = @aged WHERE server_id = @server_id", ct, ("server_id", ServerId), ("aged", aged));
        Assert.Null(await ServedAsync(store, window.Start, window.End, ct));
        var viaRaw = await ReadWithoutHeartbeatAsync(store, ct);
        Assert.False(expected["past 30 days / grid"].SequenceEqual(viaRaw["past 30 days / grid"]), "the late rows changed nothing in the 30-day grid, so the test proves nothing");
        AssertSame(viaRaw, await ReadAllAsync(store.Viewer, ct), "with an aged heartbeat and a late row no mark covers");

        /* A current service's tick: the heartbeat had aged, so it marks the day from the row itself and builds it again. */
        var next = await TickAsync(store, KeepEverything, ct, TickNow.AddHours(1));
        Assert.True(next.Built >= 1);
        Assert.Equal(0L, await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND rebuild_needed", ct, ("server_id", ServerId)));
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(window.Start, window.End), await ServedAsync(store, window.Start, window.End, ct));
        AssertSame(viaRaw, await ReadAllAsync(store.Viewer, ct), "after the tick rebuilt the late row's day");

        /* A tick with nothing to build still moves the heartbeat. */
        var idleNow = TickNow.AddHours(2);
        await ExecuteAsync(store.Connection, "UPDATE collect.query_store_closed_day_tick SET ticked_at = @before WHERE server_id = @server_id", ct, ("server_id", ServerId), ("before", idleNow.AddHours(-1)));
        var idle = await QueryStoreClosedDayRollup.RunTickAsync(store.Postgres, idleNow, KeepEverything, NullLogger.Instance, ct);
        Assert.Equal(0, idle.Built);
        Assert.Equal(idleNow - QueryStoreClosedDayRollup.HeartbeatMargin, await ScalarAsync<DateTime>(store.Connection,
            "SELECT ticked_at FROM collect.query_store_closed_day_tick WHERE server_id = @server_id", ct, ("server_id", ServerId)));
    }

    /// <summary>Every window read with the server's heartbeat gone, which sends every read to the interval table; the heartbeat comes back.</summary>
    private static async Task<Dictionary<string, List<string>>> ReadWithoutHeartbeatAsync(Store store, CancellationToken ct)
    {
        var beat = await ScalarAsync<DateTime>(store.Connection, "SELECT ticked_at FROM collect.query_store_closed_day_tick WHERE server_id = @server_id", ct, ("server_id", ServerId));
        await ExecuteAsync(store.Connection, "DELETE FROM collect.query_store_closed_day_tick WHERE server_id = @server_id", ct, ("server_id", ServerId));
        var raw = await ReadAllAsync(store.Viewer, ct);
        await ExecuteAsync(store.Connection, "INSERT INTO collect.query_store_closed_day_tick (server_id, ticked_at) VALUES (@server_id, @beat)", ct, ("server_id", ServerId), ("beat", beat));
        return raw;
    }

    [Fact]
    public async Task ARollupStatementThatFaults_ReadsTheIntervalTableInstead_WithTheSameAnswer()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9111 closed-day rollup live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);

        var expected = await ReadAllAsync(store.Viewer, ct);
        await TickAsync(store, KeepEverything, ct);
        Assert.NotNull(await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));
        AssertSame(expected, await ReadAllAsync(store.Viewer, ct), "after the build");

        /* The grid's daily table goes: the grid's statements fault on it, and the grid reads the interval table. The slicer still reads its hourly rollup. */
        await ExecuteAsync(store.Connection, "DROP TABLE collect.query_store_grid_daily", ct);
        AssertSame(expected, await ReadAllAsync(store.Viewer, ct), "with the daily rollup gone");

        /* Then the slicer's hourly table goes too. */
        await ExecuteAsync(store.Connection, "DROP TABLE collect.query_store_slicer_hourly", ct);
        AssertSame(expected, await ReadAllAsync(store.Viewer, ct), "with both rollups gone");
    }

    /// <summary>The marker the grid's and slicer's fallback line carries when a read that held no rollup gives up on the interval table.</summary>
    private const string TableFallbackMarker = "the interval-table read failed";

    /// <summary>The marker of the line a read writes when it starts again on the interval table after a rollup statement faulted.</summary>
    private const string RollupRetryMarker = "closed-day rollup read failed";

    private readonly string _logDir =
        Path.Combine(Path.GetTempPath(), "darling-viewerlog-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            ViewerLogger.Shutdown();
            if (Directory.Exists(_logDir))
            {
                Directory.Delete(_logDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort test cleanup. */
        }
    }

    private static string LogText()
    {
        ViewerLogger.Flush();
        var file = ViewerLogger.GetCurrentLogFile();
        return File.Exists(file) ? File.ReadAllText(file) : "";
    }

    /// <summary>
    /// Holds an exclusive lock on each of <paramref name="tables"/> (of the collect schema) in a transaction of its own, so a read of one
    /// waits for the lock until the store's own deadline cancels it. It makes a read fail with no role: a role is cluster-wide, and
    /// creating or dropping one takes locks on shared catalogs that other live classes running together also take (#9159).
    /// </summary>
    private static async Task<LockHolder> HoldLocksAsync(Store store, CancellationToken ct, params string[] tables)
    {
        var holder = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(store.Scratch.ConnectionString) { Pooling = false }.ConnectionString);
        try
        {
            await holder.OpenAsync(ct);
            var transaction = await holder.BeginTransactionAsync(ct);
            foreach (var table in tables)
            {
                await ExecuteAsync(holder, "LOCK TABLE collect." + table + " IN ACCESS EXCLUSIVE MODE", ct);
            }

            return new LockHolder(holder, transaction);
        }
        catch
        {
            await holder.DisposeAsync();
            throw;
        }
    }

    /// <summary>The store's connection string with one session setting, which only the connection a read uses carries.</summary>
    private static string WithSessionSetting(Store store, string setting) =>
        new NpgsqlConnectionStringBuilder(store.Scratch.ConnectionString) { Options = "-c " + setting }.ConnectionString;

    private sealed class LockHolder(NpgsqlConnection connection, NpgsqlTransaction transaction) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await transaction.RollbackAsync(CancellationToken.None);
            await transaction.DisposeAsync();
            await connection.DisposeAsync();
        }
    }

    /// <summary>
    /// A rollup read the store cancels at its own statement_timeout arrives as a PostgresException (57014), not a client TimeoutException.
    /// It is a deadline: the read gives up on the table and reads raw history, and it does not first read the interval table, which
    /// would only wait another deadline. The grid and the slicer each leave the one fallback line and no rollup-retry line.
    /// </summary>
    [Fact]
    public async Task ARollupReadThatHitsTheStoresStatementTimeout_FailsOnce_AndIsNotRetriedOnTheIntervalTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9111 closed-day rollup live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);
        await TickAsync(store, KeepEverything, ct);
        Assert.NotNull(await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));

        /* Both rollup tables are locked, so the grid's statement and the slicer's each wait on the lock until the connection's statement_timeout cancels them. */
        await using var locks = await HoldLocksAsync(store, ct, "query_store_grid_daily", "query_store_slicer_hourly");
        ViewerLogger.Initialize(_logDir);
        await using var viewer = new ViewerDataService(WithSessionSetting(store, "statement_timeout=2000"));

        var grid = await viewer.GetQueryStoreTopQueriesWithReachAsync(ServerId, DataEnd.AddDays(-30), DataEnd, Top, null, literalEndUtc: null, tags: null, ct);
        Assert.Null(grid.Plan);
        await viewer.GetQueryStoreSlicerDataAsync(ServerId, DataEnd.AddDays(-30), DataEnd, null, ct);

        var log = LogText();
        Assert.Equal(2, CountOf(log, TableFallbackMarker));
        Assert.DoesNotContain(RollupRetryMarker, log, StringComparison.Ordinal);
    }

    /// <summary>
    /// A fault in a read that holds no rollup (no day of the window is built) is not the rollups' to retry: the interval-table read runs
    /// once, then the read goes to raw history. The grid and the slicer each leave the one fallback line and no rollup-retry line.
    /// </summary>
    [Fact]
    public async Task AFaultedIntervalTableRead_WithNoBuiltDayInTheWindow_RunsOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9111 closed-day rollup live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);
        Assert.Null(await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));

        /* The interval table loses a measure column, so a statement that ranks its rows faults. The read-source decision names no measure
           column and still runs, and the store is this test's own database. */
        await ExecuteAsync(store.Connection, "ALTER TABLE collect.query_store_interval_wide RENAME COLUMN avg_cpu_time_us TO avg_cpu_time_us_gone", ct);
        ViewerLogger.Initialize(_logDir);
        await using var viewer = new ViewerDataService(store.Scratch.ConnectionString);

        var grid = await viewer.GetQueryStoreTopQueriesWithReachAsync(ServerId, DataEnd.AddDays(-30), DataEnd, Top, null, literalEndUtc: null, tags: null, ct);
        Assert.Null(grid.Plan);
        await viewer.GetQueryStoreSlicerDataAsync(ServerId, DataEnd.AddDays(-30), DataEnd, null, ct);

        var log = LogText();
        Assert.Equal(2, CountOf(log, TableFallbackMarker));
        Assert.DoesNotContain(RollupRetryMarker, log, StringComparison.Ordinal);
    }

    private static int CountOf(string text, string marker)
    {
        var count = 0;
        for (var at = text.IndexOf(marker, StringComparison.Ordinal); at >= 0; at = text.IndexOf(marker, at + marker.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    [Fact]
    public async Task Retention_RemovesDaysFromTheRollups_AndARaisedRetentionDoesNotBringThemBack()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9111 closed-day rollup live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);

        var expected = await ReadAllAsync(store.Viewer, ct);
        await TickAsync(store, KeepEverything, ct);
        AssertSame(expected, await ReadAllAsync(store.Viewer, ct), "after the build");

        /* Retention shorter than the 30-day window: the first day kept is the retention edge's own date (#9287). */
        const int shortRetention = 20;
        var firstKept = DateOnly.FromDateTime(TickNow.AddDays(-shortRetention));
        var lowered = await TickAsync(store, shortRetention, ct);
        Assert.True(lowered.DaysRemoved > 0, "lowering retention removed no day");
        Assert.Equal(0L, await ScalarAsync<long>(store.Connection,
            "SELECT (SELECT COUNT(*) FROM collect.query_store_grid_daily WHERE server_id = @server_id AND day_utc < @kept) + (SELECT COUNT(*) FROM collect.query_store_slicer_hourly WHERE server_id = @server_id AND hour_utc < @kept)", ct,
            ("server_id", ServerId), ("kept", firstKept)));
        Assert.Equal(firstKept.AddDays(-1).ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture), await ScalarAsync<string>(store.Connection,
            "SELECT CAST(max(day_utc) AS text) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND removed", ct, ("server_id", ServerId)));
        Assert.Equal(1L, await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND removed", ct, ("server_id", ServerId)));

        /* The window reaches below the first day kept: those days come from the interval table, the kept days from the rollups. */
        var served = await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct);
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(firstKept.ToDateTime(TimeOnly.MinValue), DataEnd), served);
        AssertSame(expected, await ReadAllAsync(store.Viewer, ct), "with retention shorter than the window");

        /* Retention raised again: no day at or before the newest removed day is built again. */
        var raised = await TickAsync(store, KeepEverything, ct);
        Assert.Equal(0, raised.Built);
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(firstKept.ToDateTime(TimeOnly.MinValue), DataEnd), await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));
        AssertSame(expected, await ReadAllAsync(store.Viewer, ct), "after retention was raised");
    }

    /// <summary>#9287: the retention edge the edge-day tests use: ten days before the tick's clock, at midnight.</summary>
    private static readonly DateTime EdgeDay = TickNow.AddDays(-10);

    private const string IntervalSumSql = "SELECT COALESCE(SUM(execution_count), 0)::bigint FROM collect.query_store_interval_wide WHERE server_id = @server_id AND COALESCE(interval_start_time_utc, collection_time) >= @from AND COALESCE(interval_start_time_utc, collection_time) < @to";
    private const string RollupSumSql = "SELECT COALESCE(SUM(execution_count), 0)::bigint FROM collect.query_store_grid_daily WHERE server_id = @server_id AND day_utc = @day";

    /// <summary>Gives <paramref name="day"/>'s rows from the first twenty queries a <c>first_execution_time</c> 25 hours before the day, as a
    /// row with no interval start can have, so a purge on <c>first_execution_time</c> reaches them while the day is still inside retention.</summary>
    private static Task KeyEarlyAsync(Store store, DateTime day, CancellationToken ct) => ExecuteAsync(store.Connection,
        "UPDATE collect.query_store_interval_wide SET first_execution_time = @day - interval '25 hours' WHERE server_id = @server_id AND interval_start_time_utc >= @day AND interval_start_time_utc < @day + interval '1 day' AND query_id < 1020", ct,
        ("server_id", ServerId), ("day", day));

    /// <summary>The purge on <c>first_execution_time</c>: every row keyed before <paramref name="edge"/> goes.</summary>
    private static Task PurgeBelowAsync(Store store, DateTime edge, CancellationToken ct) => ExecuteAsync(store.Connection,
        "DELETE FROM collect.query_store_interval_wide WHERE server_id = @server_id AND first_execution_time < @edge", ct,
        ("server_id", ServerId), ("edge", edge));

    /// <summary>
    /// #9287: a built day one or two days inside the retention edge stays served from its rollup after the purge has dropped the raw rows
    /// it keyed below the edge. The read answers with the day's as-collected totals. The old margin removed such a day from the rollups.
    /// </summary>
    [Fact]
    public async Task ABuiltDayNearTheRetentionEdge_StaysServedFromItsRollup_AfterThePurgeDroppedItsEarlyKeyedRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9287 edge-day live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);

        var nearEdge = EdgeDay.AddDays(1);
        await KeyEarlyAsync(store, nearEdge, ct);
        var collected = await ScalarAsync<long>(store.Connection, IntervalSumSql, ct, ("server_id", ServerId), ("from", EdgeDay), ("to", DataEnd));

        await TickAsync(store, KeepEverything, ct);
        var collectedForTheDay = await ScalarAsync<long>(store.Connection, IntervalSumSql, ct, ("server_id", ServerId), ("from", nearEdge), ("to", nearEdge.AddDays(1)));
        Assert.Equal(collectedForTheDay, await ScalarAsync<long>(store.Connection, RollupSumSql, ct, ("server_id", ServerId), ("day", DateOnly.FromDateTime(nearEdge))));

        await PurgeBelowAsync(store, EdgeDay, ct);
        var raw = await ScalarAsync<long>(store.Connection, IntervalSumSql, ct, ("server_id", ServerId), ("from", EdgeDay), ("to", DataEnd));
        Assert.True(raw < collected, "the purge dropped no row of the day, so the test proves nothing");

        /* The next tick removes nothing of the day and builds nothing of it again. */
        await TickAsync(store, 10, ct);
        var served = await ServedAsync(store, EdgeDay, DataEnd, ct);
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(EdgeDay, DataEnd), served);
        /* The rollup still holds the day as collected, though the interval table no longer does. */
        Assert.Equal(collectedForTheDay, await ScalarAsync<long>(store.Connection, RollupSumSql, ct, ("server_id", ServerId), ("day", DateOnly.FromDateTime(nearEdge))));
    }

    /// <summary>#9287: a rollup day past the retention edge that the cleanup has not removed yet is never taken by a read.</summary>
    [Fact]
    public async Task ARollupDayPastTheRetentionEdge_IsNeverServed_WhenTheCleanupHasNotRunYet()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9287 edge-day live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);

        await TickAsync(store, KeepEverything, ct);
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(DataEnd.AddDays(-30), DataEnd), await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));

        /* The same rollup rows, read at a clock whose ten-day retention starts at the edge: no GC ran, and the days before it are not taken. */
        QueryStoreClosedDayRollup.SetReadClockForTests(store.Connection.Database, TickNow, 10);
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(EdgeDay, DataEnd), await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));

        /* An edge that falls inside a day leaves that day out as well: only whole days inside retention are served. */
        QueryStoreClosedDayRollup.SetReadClockForTests(store.Connection.Database, TickNow.AddHours(5), 10);
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(EdgeDay.AddDays(1), DataEnd), await ServedAsync(store, DataEnd.AddDays(-30), DataEnd, ct));
    }

    /// <summary>#9287: the cleanup keeps a day wholly inside retention that the old four-day margin removed, and removes a day wholly past the edge.</summary>
    [Fact]
    public async Task TheCleanup_KeepsADayWhollyInsideRetention_AndRemovesADayWhollyPastTheEdge()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9287 edge-day live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);

        await TickAsync(store, KeepEverything, ct);
        var removed = await TickAsync(store, 10, ct);
        Assert.True(removed.DaysRemoved > 0, "lowering retention removed no day");

        var edge = DateOnly.FromDateTime(EdgeDay);
        Assert.Equal(0L, await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_grid_daily WHERE server_id = @server_id AND day_utc < @edge", ct, ("server_id", ServerId), ("edge", edge)));
        for (var offset = 0; offset < 4; offset++)
        {
            Assert.True(await ScalarAsync<long>(store.Connection,
                "SELECT COUNT(*) FROM collect.query_store_grid_daily WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", ServerId), ("day", edge.AddDays(offset))) > 0,
                $"the day {offset} day(s) inside the edge was removed");
        }

        Assert.Equal(edge.AddDays(-1).ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture), await ScalarAsync<string>(store.Connection,
            "SELECT CAST(max(day_utc) AS text) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND removed", ct, ("server_id", ServerId)));
    }

    /// <summary>
    /// #9287: a built day the purge may already have thinned (<see cref="QueryStoreClosedDayRollup.MayBeThinned"/>) is never rebuilt from the
    /// interval table, whatever the oldest row the table holds says. The purge dropped the rows keyed before the day, the oldest row left
    /// sits two hours before the day starts (which a floor test would call a whole day), and a rebuild mark is set. The tick keeps the
    /// rollup as collected, clears the mark, and the read still takes the day from it. A day never built stays with the interval table.
    /// </summary>
    [Fact]
    public async Task ABuiltDayInTheMarginZone_IsNeverRebuiltThinner_WhateverTheOldestRowSays_AndANeverBuiltOneIsNotBuilt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9287 edge-day live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);

        var day = EdgeDay.AddDays(1);
        var dayKey = DateOnly.FromDateTime(day);
        Assert.True(QueryStoreClosedDayRollup.MayBeThinned(dayKey, TickNow, 10), "the day is outside the margin zone, so the test proves nothing");
        await KeyEarlyAsync(store, day, ct);
        await TickAsync(store, KeepEverything, ct);
        var built = await ScalarAsync<long>(store.Connection, RollupSumSql, ct, ("server_id", ServerId), ("day", dayKey));

        /* The day was built while it was well inside retention. The purge takes its early-keyed rows and leaves the table's oldest row two hours before the day starts. */
        await PurgeBelowAsync(store, day.AddHours(-2), ct);
        Assert.True(await ScalarAsync<long>(store.Connection, IntervalSumSql, ct, ("server_id", ServerId), ("from", day), ("to", day.AddDays(1))) < built, "the purge dropped no row of the day");
        var oldest = await ScalarAsync<DateTime>(store.Connection, "SELECT MIN(first_execution_time) FROM collect.query_store_interval_wide WHERE server_id = @server_id", ct, ("server_id", ServerId));
        Assert.True(oldest >= day.AddHours(-2) && oldest < day, $"the oldest row is at {oldest:O}, not just before the day");
        await ExecuteAsync(store.Connection, "UPDATE collect.query_store_closed_day_built SET rebuild_needed = true WHERE server_id = @server_id AND day_utc = @day", ct,
            ("server_id", ServerId), ("day", dayKey));
        /* A day in the zone with no built row at all. */
        var neverBuilt = DateOnly.FromDateTime(EdgeDay);
        await ExecuteAsync(store.Connection, "DELETE FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", ServerId), ("day", neverBuilt));
        await ExecuteAsync(store.Connection, "DELETE FROM collect.query_store_grid_daily WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", ServerId), ("day", neverBuilt));

        await TickAsync(store, 10, ct);

        Assert.Equal(built, await ScalarAsync<long>(store.Connection, RollupSumSql, ct, ("server_id", ServerId), ("day", dayKey)));
        Assert.False(await ScalarAsync<bool>(store.Connection,
            "SELECT rebuild_needed FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", ServerId), ("day", dayKey)));
        Assert.Equal(0L, await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", ServerId), ("day", neverBuilt)));
        var served = await ServedAsync(store, EdgeDay, DataEnd, ct);
        Assert.True(served is { } run && run.Start <= day && run.End > day, "the day is no longer served from its rollup");
        var source = QueryStoreClosedDayRollup.DescribeSource(day, day.AddDays(2), served, QueryStoreClosedDayRollup.SourceReason.Served,
            QueryStoreClosedDayRollup.HoldsEdgeDays(store.Connection, served));
        Assert.Contains(QueryStoreClosedDayRollup.EdgeDaysNote, source.Describe(), StringComparison.Ordinal);
    }

    /// <summary>#9287: a day outside the margin zone is rebuilt as before, and a run of such days alone does not say "edge days".</summary>
    [Fact]
    public async Task ABuiltDayOutsideTheMarginZone_IsRebuiltFromTheIntervalTable_AndItsRunSaysNothingOfEdgeDays()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9287 edge-day live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);

        var day = EdgeDay.AddDays(3);
        var dayKey = DateOnly.FromDateTime(day);
        Assert.False(QueryStoreClosedDayRollup.MayBeThinned(dayKey, TickNow, 10), "the day is inside the margin zone, so the test proves nothing");
        await TickAsync(store, 10, ct);
        await ExecuteAsync(store.Connection, "DELETE FROM collect.query_store_interval_wide WHERE server_id = @server_id AND interval_start_time_utc >= @day AND interval_start_time_utc < @day + interval '1 day' AND execution_count > 0 AND (query_id % 2) = 0", ct,
            ("server_id", ServerId), ("day", day));
        var remaining = await ScalarAsync<long>(store.Connection, IntervalSumSql, ct, ("server_id", ServerId), ("from", day), ("to", day.AddDays(1)));
        await ExecuteAsync(store.Connection, "UPDATE collect.query_store_closed_day_built SET rebuild_needed = true WHERE server_id = @server_id AND day_utc = @day", ct, ("server_id", ServerId), ("day", dayKey));

        await TickAsync(store, 10, ct);

        Assert.Equal(remaining, await ScalarAsync<long>(store.Connection, RollupSumSql, ct, ("server_id", ServerId), ("day", dayKey)));
        var served = await ServedAsync(store, day, DataEnd, ct);
        Assert.False(QueryStoreClosedDayRollup.HoldsEdgeDays(store.Connection, served));
    }

    /// <summary>
    /// #9221: the worker's own closed-day pass, with no fleet override row. Its clutter retention used to be looked up under the TABLE
    /// name <c>query_store_stats</c>, the schedule defaults key the collector as <c>query_store</c>, so the lookup threw before the tick started
    /// and no server got a heartbeat. Two servers hold a coverage claim, and each gets its row.
    /// </summary>
    [Fact]
    public async Task TheWorkersClosedDayPass_WithNoFleetOverrideRow_WritesOneHeartbeatPerServerWithACoverageClaim()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);
        const int otherServerId = ServerId - 1;
        await QueryStoreIntervalWideGridLiveTests.SeedGridAsync(store.Runner, otherServerId, Anchor, ct);
        await ExecuteAsync(store.Connection, "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", ct,
            ("server_id", otherServerId), ("value", Anchor.AddDays(-90)));
        await ExecuteAsync(store.Connection, "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (@server_id, 'rollup-9221', TRUE) ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct,
            ("server_id", otherServerId));
        Assert.Equal(0L, await ScalarAsync<long>(store.Connection, "SELECT COUNT(*) FROM collect.query_store_closed_day_tick", ct));

        var worker = SelfAlertFleetPassTests.MakeWorker();
        worker.StoreForTests = store.Postgres;
        worker.ScheduleOverridesForTest = Array.Empty<ScheduleOverride>();
        await worker.RunQueryStoreClosedDayTickAsync(ct);

        Assert.Equal(2L, await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_closed_day_tick WHERE server_id IN (@a, @b)", ct, ("a", ServerId), ("b", otherServerId)));
        Assert.Equal(2L, await ScalarAsync<long>(store.Connection,
            "SELECT COUNT(*) FROM collect.query_store_closed_day_tick WHERE server_id IN (@a, @b) AND ticked_at IS NOT NULL", ct, ("a", ServerId), ("b", otherServerId)));
    }

    /// <summary>
    /// #9221: the retention the clutter step prunes on is the one the purge applies to <c>query_store_stats</c>' raw rows, with no fleet
    /// override row and with one. The purge builds that argument for each catalog entry from its collector name, so the expected value
    /// here is built the same way from the entry whose target table it is.
    /// </summary>
    [Fact]
    public void TheClutterStepsRetention_IsThePurgesForQueryStoreStats_WithAndWithoutAFleetOverrideRow()
    {
        static int PurgeDays(IReadOnlyList<ScheduleOverride> overrides)
        {
            var owner = Assert.Single(CollectorCatalog.All, c => c.TargetTable == "query_store_stats");
            return DarlingRetention.EffectivePurgeRetentionDays(owner.Name, StoreConfigProvider.ResolveFleetRetentionDays(owner.Name, overrides));
        }

        var entry = Assert.Single(CollectorCatalog.All, c => c.TargetTable == "query_store_stats");
        Assert.Equal(PurgeDays(Array.Empty<ScheduleOverride>()), DarlingRetention.QueryStoreStatsPurgeRetentionDays(Array.Empty<ScheduleOverride>()));
        Assert.Equal(CollectorScheduleDefaults.All[entry.Name].RetentionDays, DarlingRetention.QueryStoreStatsPurgeRetentionDays(Array.Empty<ScheduleOverride>()));

        var fleet = new[] { new ScheduleOverride(null, entry.Name, null, 45, true) };
        Assert.Equal(PurgeDays(fleet), DarlingRetention.QueryStoreStatsPurgeRetentionDays(fleet));
        Assert.Equal(45, DarlingRetention.QueryStoreStatsPurgeRetentionDays(fleet));

        var perServerOnly = new[] { new ScheduleOverride(7, entry.Name, null, 45, true) };
        Assert.Equal(PurgeDays(perServerOnly), DarlingRetention.QueryStoreStatsPurgeRetentionDays(perServerOnly));
    }

    /// <summary>
    /// #9363: a whole closed day with no built row names why in the slow-load line's source, and a built day that sits outside the longest run
    /// says so. The statement that finds the built days already returns every one of them, so no further read is made to tell the two apart.
    /// </summary>
    [Fact]
    public async Task AWholeClosedDayWithoutARollup_IsNamedInTheSource_AndABuiltDayOutsideTheRunSaysSo()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9363 source-day live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);
        await TickAsync(store, KeepEverything, ct);

        /* Day -10 of a 30 day window loses its built row: 20 built days lie before it and 9 after it, so the longer run is the one before. */
        var missing = DateOnly.FromDateTime(DataEnd.AddDays(-10));
        await ExecuteAsync(store.Connection, "DELETE FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc = @day", ct,
            ("server_id", ServerId), ("day", missing));

        var start = DataEnd.AddDays(-30);
        var (served, reason, built) = await QueryStoreClosedDayRollup.ResolveServedDaysWithReasonAsync(store.Connection, null, ServerId, start, DataEnd, 30, ct);
        Assert.Equal(new QueryStoreClosedDayRollup.ServedDays(start, DataEnd.AddDays(-10)), served);
        Assert.Equal(QueryStoreClosedDayRollup.SourceReason.Served, reason);
        Assert.DoesNotContain(missing, built);
        Assert.Equal(29, built.Count);

        var source = QueryStoreClosedDayRollup.DescribeSourceFor(store.Connection, start, DataEnd, served, reason, built);

        var notes = source.IntervalNotes!.ToDictionary(n => n.Day, n => n.Label);
        Assert.Equal(10, notes.Count);
        Assert.Equal("no rollup for this day", notes[missing]);
        for (var day = missing.AddDays(1); day < DateOnly.FromDateTime(DataEnd); day = day.AddDays(1))
        {
            Assert.Equal("built but outside the longest run", notes[day]);
        }

        Assert.Contains($"20 days from rollups ({DateOnly.FromDateTime(start):yyyy-MM-dd} to {missing.AddDays(-1):yyyy-MM-dd})", source.Describe(), StringComparison.Ordinal);
        Assert.Contains($"{missing:yyyy-MM-dd} no rollup for this day", source.Describe(), StringComparison.Ordinal);
    }

    /// <summary>
    /// #9363: the Viewer's grid and slicer reads pass the built days through to the source they describe. A built day outside the longest run
    /// says so on both reads, which a call site that dropped the built days would print as "no rollup for this day".
    /// </summary>
    [Fact]
    public async Task TheGridAndSlicerReads_NameABuiltDayOutsideTheRun_FromTheBuiltDaysTheyResolved()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9363 call site live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeedAsync(ct);
        await TickAsync(store, KeepEverything, ct);

        var missing = DateOnly.FromDateTime(DataEnd.AddDays(-10));
        await ExecuteAsync(store.Connection, "DELETE FROM collect.query_store_closed_day_built WHERE server_id = @server_id AND day_utc = @day", ct,
            ("server_id", ServerId), ("day", missing));

        var start = DataEnd.AddDays(-30);
        var grid = await store.Viewer.GetQueryStoreTopQueriesWithReachAsync(ServerId, start, DataEnd, Top, null, literalEndUtc: null, tags: null, ct);
        var slicer = await store.Viewer.GetQueryStoreSlicerWithReachAsync(ServerId, start, DataEnd, null, ct);
        Assert.True(grid.Plan is { UseTable: true, Source: not null }, "the grid read did not take the interval-table route");
        Assert.True(slicer.Plan is { UseTable: true, Source: not null }, "the slicer read did not take the interval-table route");

        foreach (var source in new[] { grid.Plan!.Value.Source!, slicer.Plan!.Value.Source! })
        {
            var notes = source.IntervalNotes!.ToDictionary(n => n.Day, n => n.Label);
            Assert.Equal("no rollup for this day", notes[missing]);
            Assert.Equal("built but outside the longest run", notes[missing.AddDays(1)]);
        }
    }
}
