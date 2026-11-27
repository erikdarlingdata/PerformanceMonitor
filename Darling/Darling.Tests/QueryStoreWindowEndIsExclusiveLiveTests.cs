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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9100: a Query Store window is the half-open range from its start to <c>as_of</c>. An interval whose start equals
/// <c>as_of</c> belongs to the next window, so a one-hour window asked on the hour does not carry the hour that begins at its end,
/// and an interval that starts exactly at the window's start stays in. Three intervals start at <c>as_of</c> minus an hour, at
/// <c>as_of</c> and at <c>as_of</c> plus an hour; each read returns only the first. The top read goes through the tool with
/// <c>as_of</c>, and the raw and interval-table statements of the top read and of the aborts read run against the same rows.
/// The Viewer's grid, interval-table grid and slicer, and the duration trend, read the same rows over the same window. The
/// fleet summary's Query Store tail ends the same way, for an interval placed by its start and for a row with no interval start.
/// </summary>
/* #1776 own-store: reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and then
   works entirely inside it, the same shape as QueryStoreTopWindowPlacementLiveTests. */
public sealed class QueryStoreWindowEndIsExclusiveLiveTests
{
    private const string ServerName = "query-store-window-end-9100";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const long AtWindowStart = 1;
    private const long AtAsOf = 2;
    private const long AfterAsOf = 3;

    private const string FleetHash = "0xFLEET9100";

    private static readonly DateTime AsOf = new(2026, 11, 13, 1, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = AsOf.AddHours(-1);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheTool_OnAnHourlyAsOf_CountsOnlyTheIntervalsStartedBeforeIt_AndKeepsTheOneAtTheWindowStart()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9100 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var csb = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var source = NpgsqlDataSource.Create(csb.ConnectionString);
        await using var connection = new NpgsqlConnection(csb.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await SeedRawAsync(connection, "Regular", ct);

        var json = await DarlingMcpDataTools.GetQueryStoreTop(source, ServerName, hours_back: 1, as_of: "2026-11-13T01:00:00Z", cancellationToken: ct);

        var rows = JsonNode.Parse(json)!["queries"]!.AsArray();
        Assert.Equal(new long[] { AtWindowStart }, rows.Select(r => (long)r!["query_id"]!).OrderBy(i => i).ToArray());
        Assert.Equal(10, (long)rows[0]!["execution_count"]!);
    }

    [Fact]
    public async Task TheRawRead_StopsBeforeAsOf_AndKeepsTheWindowStart()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9100 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedRawAsync(connection, "Regular", ct);

        var ids = await TopIdsAsync(connection, DarlingDataReader.QueryStoreTopSql, null, ct);

        Assert.Equal(new long[] { AtWindowStart }, ids);
    }

    [Fact]
    public async Task TheIntervalTableRead_StopsBeforeAsOf_AndKeepsTheWindowStart()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9100 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedWideAsync(connection, "Regular", ct);

        var ids = await TopIdsAsync(connection, DarlingDataReader.QueryStoreTopTableSql, null, ct);

        Assert.Equal(new long[] { AtWindowStart }, ids);
    }

    [Fact]
    public async Task TheAbortsRawRead_StopsBeforeAsOf_AndKeepsTheWindowStart()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9100 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedRawAsync(connection, "Aborted", ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var aborts = await DarlingQueryStoreAbortsReader.ReadAsync(postgres, ServerId, WindowStart, AsOf, 20, DatabaseFilter.All, ct);

        Assert.Null(aborts.Table);   /* a one hour window reads the raw tier */
        Assert.Equal(new long[] { AtWindowStart }, aborts.Rows.Select(r => r.QueryId).OrderBy(i => i).ToArray());
    }

    [Fact]
    public async Task TheAbortsIntervalTableRead_StopsBeforeAsOf_AndKeepsTheWindowStart()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9100 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedWideAsync(connection, "Aborted", ct);

        await using var command = new NpgsqlCommand(DarlingQueryStoreAbortsReader.TableSql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowStart, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = AsOf, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 20 });
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        var ids = new List<long>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            ids.Add(reader.GetInt64(reader.GetOrdinal("query_id")));
        }

        Assert.Equal(new long[] { AtWindowStart }, ids.OrderBy(i => i).ToArray());
    }

    [Fact]
    public async Task TheViewerGridAndSlicer_StopBeforeTheEnd_AndKeepTheWindowStart()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9100 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedRawAsync(connection, "Regular", ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var grid = await viewer.GetQueryStoreTopQueriesAsync(ServerId, WindowStart, AsOf, cancellationToken: ct);
        Assert.Equal(new long[] { AtWindowStart }, grid.Select(r => r.QueryId).OrderBy(i => i).ToArray());

        var slicer = await viewer.GetQueryStoreSlicerDataAsync(ServerId, WindowStart, AsOf, null, ct);
        Assert.Equal(1, slicer.Sum(b => b.SessionCount));
    }

    [Fact]
    public async Task TheViewerGridIntervalTableRead_StopsBeforeAnEnd_ClosedOrOpen()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9100 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedWideAsync(connection, "Regular", ct);

        await using var command = new NpgsqlCommand(ViewerDataService.QueryStoreTopTableSql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowStart, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = AsOf, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 20 });
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopFill.FirstCandidates(20) });
        await using var reader = await command.ExecuteReaderAsync(ct);
        var (rows, _) = await TopFill.ReadPageAsync(reader, reader.GetOrdinal("page_ord"), r => r.GetInt64(r.GetOrdinal("query_id")), ct);

        Assert.Equal(new long[] { AtWindowStart }, rows.OrderBy(i => i).ToArray());
    }

    [Fact]
    public async Task TheDurationTrend_PlacesOnlyTheIntervalStartedBeforeTheEnd_AndKeepsTheWindowStart()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9100 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedRawAsync(connection, "Regular", ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var points = await DarlingTrendReader.GetQueryStoreDurationTrendAsync(
            postgres, ServerId, WindowStart, AsOf, QueryStoreTrendRouting.QueryStoreTrendRoute.RawOnly, DatabaseFilter.All, ct);

        var point = Assert.Single(points);
        Assert.Equal(WindowStart, point.CollectionTime);
    }

    [Fact]
    public async Task TheRollupRoutedDurationTrend_PlacesOnlyTheIntervalStartedBeforeTheEnd_ThroughItsRawTail()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9100 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await SeedRawAsync(connection, "Regular", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* The rollup route with its raw boundary at the window's start: nothing is materialized below it, so the whole window is the raw tail. */
        var csb = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var postgres = NpgsqlDataSource.Create(csb.ConnectionString);
        var route = new QueryStoreTrendRouting.QueryStoreTrendRoute(true, WindowStart, null);
        var points = await DarlingTrendReader.GetQueryStoreDurationTrendAsync(postgres, ServerId, WindowStart, AsOf, route, DatabaseFilter.All, ct);

        var point = Assert.Single(points);
        Assert.Equal(WindowStart, point.CollectionTime);
    }

    [Fact]
    public async Task TheViewerItemTimeline_PlotsOnlyTheIntervalStartedBeforeTheEnd_AndKeepsTheWindowStart()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9100 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* One query and plan, an interval at the window's start and one at its end. */
        const long query = 7;
        var interval = 1;
        foreach (var started in new[] { WindowStart, AsOf })
        {
            await ExecAsync(connection, $"""
                INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
                    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
                    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc, last_execution_time)
                VALUES ({interval}, timestamp '{started.AddMinutes(30):yyyy-MM-dd HH:mm}', {ServerId}, '{ServerName}', 'db0', {query}, {query}, 'Regular',
                        timestamp '{started.AddMinutes(5):yyyy-MM-dd HH:mm}', 'mod', md5('h{query}'), 10, 500, 300, 900, 700, NULL, {interval},
                        timestamp '{started:yyyy-MM-dd HH:mm}', timestamp '{started.AddMinutes(25):yyyy-MM-dd HH:mm}')
                """, ct);
            interval++;
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var points = await viewer.GetQueryStoreItemTimelineAsync(ServerId, "db0", query, query, WindowStart, AsOf, ct);

        var point = Assert.Single(points);
        Assert.Equal(WindowStart, point.PointTime);
    }

    [Fact]
    public async Task TheFleetSummary_CountsOnlyTheQueryStoreRowsStartedBeforeTheEnd_AndKeepsTheWindowStart()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9100 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var csb = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var source = NpgsqlDataSource.Create(csb.ConnectionString);
        await using var connection = new NpgsqlConnection(csb.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        /* One query_hash, one database per row so each row answers on its own: three intervals placed by their start (at the window's
           start, at its end and an hour after it) and two rows with no interval start, placed by their collection time (inside the
           window and exactly at its end). */
        var id = 0;
        foreach (var (database, started) in new[] { ("InWindow", WindowStart), ("AtEnd", AsOf), ("AfterEnd", AsOf.AddHours(1)) })
        {
            id++;
            await ExecAsync(connection, $"""
                INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
                    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
                    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc, last_execution_time)
                VALUES ({id}, timestamp '{started.AddMinutes(30):yyyy-MM-dd HH:mm}', {ServerId}, '{ServerName}', '{database}', {id}, {id}, 'Regular',
                        timestamp '{started.AddMinutes(5):yyyy-MM-dd HH:mm}', 'mod', '{FleetHash}', 10, 500, 300, 900, 700, NULL, {id},
                        timestamp '{started:yyyy-MM-dd HH:mm}', timestamp '{started.AddMinutes(25):yyyy-MM-dd HH:mm}')
                """, ct);
        }

        foreach (var (database, collected) in new[] { ("LegacyInWindow", WindowStart.AddMinutes(10)), ("LegacyAtEnd", AsOf) })
        {
            id++;
            await ExecAsync(connection, $"""
                INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
                    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
                    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc, last_execution_time)
                VALUES ({id}, timestamp '{collected:yyyy-MM-dd HH:mm}', {ServerId}, '{ServerName}', '{database}', {id}, {id}, 'Regular',
                        timestamp '{collected.AddMinutes(-5):yyyy-MM-dd HH:mm}', 'mod', '{FleetHash}', 10, 500, 300, 900, 700, NULL, {id},
                        NULL, timestamp '{collected.AddMinutes(-1):yyyy-MM-dd HH:mm}')
                """, ct);
        }

        var json = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(source, FleetHash, 1, as_of: "2026-11-13T01:00:00Z", limit: 200, cancellationToken: ct);

        var node = JsonNode.Parse(json)!;
        Assert.Null(node["error"]);
        var databases = node["servers"]!.AsArray()
            .Where(r => (string?)r!["source"] == DarlingMcpQueryFleetSummaryTools.QueryStoreSource)
            .Select(r => (string)r!["database_name"]!)
            .OrderBy(d => d, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(new[] { "InWindow", "LegacyInWindow" }, databases);
    }

    /// <summary>One interval at the window's start, one at <c>as_of</c> and one an hour after it, each collected half an hour
    /// after it starts, as a closed interval is.</summary>
    private static (long Query, DateTime Started)[] Intervals() =>
    [
        (AtWindowStart, AsOf.AddHours(-1)),
        (AtAsOf, AsOf),
        (AfterAsOf, AsOf.AddHours(1)),
    ];

    private static async Task SeedRawAsync(NpgsqlConnection connection, string outcome, CancellationToken ct)
    {
        foreach (var (q, started) in Intervals())
        {
            await ExecAsync(connection, $"""
                INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
                    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
                    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc, last_execution_time)
                VALUES ({q}, timestamp '{started.AddMinutes(30):yyyy-MM-dd HH:mm}', {ServerId}, '{ServerName}', 'db0', {q}, {q}, '{outcome}',
                        timestamp '{started.AddMinutes(5):yyyy-MM-dd HH:mm}', 'mod', md5('h{q}'), 10, 500, 300, 900, 700, NULL, {q},
                        timestamp '{started:yyyy-MM-dd HH:mm}', timestamp '{started.AddMinutes(25):yyyy-MM-dd HH:mm}')
                """, ct);
        }
    }

    private static async Task SeedWideAsync(NpgsqlConnection connection, string outcome, CancellationToken ct)
    {
        foreach (var (q, started) in Intervals())
        {
            await ExecAsync(connection, $"""
                INSERT INTO collect.query_store_interval_wide
                    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
                     last_execution_time, query_hash, query_plan_hash, execution_count, avg_duration_us, avg_cpu_time_us,
                     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, runtime_stats_interval_id,
                     interval_start_time_utc)
                VALUES (timestamp '{started.AddMinutes(30):yyyy-MM-dd HH:mm}', {ServerId}, 'db0', {q}, {q}, '{outcome}',
                        timestamp '{started.AddMinutes(5):yyyy-MM-dd HH:mm}', timestamp '{started.AddMinutes(25):yyyy-MM-dd HH:mm}',
                        md5('h{q}'), 'ph{q}', 10, 500, 300, 100, 0, 0, 1, {q}, timestamp '{started:yyyy-MM-dd HH:mm}')
                """, ct);
        }
    }

    private static async Task<long[]> TopIdsAsync(NpgsqlConnection connection, string sql, string? outcome, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowStart, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = AsOf, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 20 });
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)outcome ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopFill.FinalCandidates(20) });
        await using var reader = await command.ExecuteReaderAsync(ct);
        var (rows, _) = await TopFill.ReadPageAsync(reader, reader.GetOrdinal("page_ord"), r => r.GetInt64(r.GetOrdinal("query_id")), ct);
        Assert.NotEmpty(rows);   /* the fixture produced rows: an empty answer is a bug in the read, not a pass */
        return rows.OrderBy(i => i).ToArray();
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
