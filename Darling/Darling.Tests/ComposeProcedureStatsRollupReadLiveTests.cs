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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #9357: the procedure_stats hourly-raw-edges FROM item, built without the restart-rows arm, gives the same value as the raw read for
/// every group (and every group-by-hour) that has a value. The route that takes this FROM item is not reachable until the guard reads its
/// ledger, so the test calls the builder directly (<see cref="ComposeCompiler.HourlyRawEdgesRelation"/>) and wraps it in the panel's own
/// shape: the aggregate with <c>FILTER (WHERE f.sample_interval_seconds IS DISTINCT FROM 0)</c>, the window, a GROUP BY. The raw side is
/// the same statement over <c>collect.procedure_stats</c>. A group whose only rows in the whole-hour middle are restart rows has a NULL
/// value in the raw read and is absent from the arm-less read; every other group is equal. Own store: each fact creates and drops its
/// own database.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class ComposeProcedureStatsRollupReadLiveTests
{
    private const int ServerIdA = -49001;
    private const int ServerIdB = -49002;
    private const string ServerA = "ProcRollupServerA";
    private const string ServerB = "ProcRollupServerB";

    private static readonly DateTime Anchor = new(2026, 1, 5, 12, 37, 21, DateTimeKind.Utc);
    private static long s_collectionId = Anchor.Ticks;

    [Theory]
    [InlineData(ComposeAggregate.Sum)]
    [InlineData(ComposeAggregate.Max)]
    [InlineData(ComposeAggregate.Min)]
    public async Task TheArmlessRead_EqualsTheRawRead_ForEveryMeasure_AndEveryGroupWithAValue(ComposeAggregate aggregate)
    {
        await using var store = await StoreAsync();
        await SeedAsync(store);

        foreach (var column in new[] { "delta_worker_time", "delta_elapsed_time", "delta_execution_count" })
        {
            var raw = await ReadAsync(store, aggregate, column, hybrid: false, byHour: false);
            var armless = await ReadAsync(store, aggregate, column, hybrid: true, byHour: false);
            AssertSameValues(raw, armless, $"{aggregate} {column} per group");
            Assert.True(armless.Count > 5, "the seeded fixture has many groups with a value");

            var rawHourly = await ReadAsync(store, aggregate, column, hybrid: false, byHour: true);
            var armlessHourly = await ReadAsync(store, aggregate, column, hybrid: true, byHour: true);
            AssertSameValues(rawHourly, armlessHourly, $"{aggregate} {column} per group and hour");
        }
    }

    [Fact]
    public async Task ARestartOnlyGroupInTheMiddle_IsNullInRaw_AndAbsentFromTheArmlessRead_AnEdgeOneStaysNullInBoth()
    {
        await using var store = await StoreAsync();
        await SeedAsync(store);

        var raw = await ReadAsync(store, ComposeAggregate.Sum, "delta_worker_time", hybrid: false, byHour: false);
        var armless = await ReadAsync(store, ComposeAggregate.Sum, "delta_worker_time", hybrid: true, byHour: false);

        var restartMid = Key(ServerIdA, "RestartMidDb", "usp_RestartMid");
        Assert.True(raw.ContainsKey(restartMid), "raw reads the restart-only group");
        Assert.Null(raw[restartMid]);
        Assert.False(armless.ContainsKey(restartMid), "the arm-less read leaves the restart-only group out");

        var restartEdge = Key(ServerIdB, "RestartEdgeDb", "usp_RestartEdge");
        Assert.True(raw.ContainsKey(restartEdge));
        Assert.Null(raw[restartEdge]);
        Assert.True(armless.ContainsKey(restartEdge), "a restart row outside the middle is read raw, as raw reads it");
        Assert.Null(armless[restartEdge]);
    }

    [Fact]
    public async Task ARestartRowWithADelta_AddsNothing_InEitherRead()
    {
        await using var store = await StoreAsync();
        await SeedAsync(store);

        var key = Key(ServerIdA, "MainDb", "usp_One");
        var raw = await ReadAsync(store, ComposeAggregate.Sum, "delta_worker_time", hybrid: false, byHour: false);
        var armless = await ReadAsync(store, ComposeAggregate.Sum, "delta_worker_time", hybrid: true, byHour: false);
        Assert.NotNull(raw[key]);
        Assert.True(raw[key] < 900_000m, "the planted restart delta of 999999 is not in the sum");
        Assert.Equal(raw[key], armless[key]);
    }

    /// <summary>The groups below run over 24 hours with restart rows mixed in. Hour h1 + 5 holds only a restart row for the main group,
    /// so that bucket is NULL in the raw per-hour read and absent from the arm-less one.</summary>
    private static async Task SeedAsync(Store store)
    {
        var (h1, h2) = (store.H1, store.H2);
        for (var h = h1; h < h2; h = h.AddHours(1))
        {
            var n = (int)(h - h1).TotalHours;
            if (n == 5)
            {
                await ProcAsync(store, ServerIdA, "MainDb", "usp_One", h.AddMinutes(25), 0, 0, 0, 0);
                continue;
            }

            await ProcAsync(store, ServerIdA, "MainDb", "usp_One", h.AddMinutes(10), 100 + n, 3 * (100 + n) + 1, 1 + n % 5, 300);
            await ProcAsync(store, ServerIdA, "MainDb", "usp_One", h.AddMinutes(40), 7 + n * 2, 5 * (7 + n * 2) + 2, 2 + n % 3, 300);
            await ProcAsync(store, ServerIdA, "MainDb", "usp_One", h.AddMinutes(25), n == 3 ? 999_999 : 0, 0, 0, 0);
            if (n % 3 == 0)
            {
                await ProcAsync(store, ServerIdB, "OtherDb", "usp_Two", h.AddMinutes(15), 50 + n, 9 * (50 + n) + 3, 4 + n, 300);
                await ProcAsync(store, ServerIdB, "OtherDb", "usp_Two", h.AddMinutes(20), 0, 0, 0, 0);
            }
        }

        /* a group of only restart rows in the middle; one only outside it; edge-only values on each side; a pre-upgrade NULL interval */
        await ProcAsync(store, ServerIdA, "RestartMidDb", "usp_RestartMid", h1.AddHours(2).AddMinutes(5), 0, 0, 0, 0);
        await ProcAsync(store, ServerIdA, "RestartMidDb", "usp_RestartMid", h1.AddHours(9).AddMinutes(5), 0, 0, 0, 0);
        await ProcAsync(store, ServerIdB, "RestartEdgeDb", "usp_RestartEdge", h1.AddMinutes(-10), 0, 0, 0, 0);
        await ProcAsync(store, ServerIdA, "EdgeOnlyDb", "usp_LeftEdge", h1.AddMinutes(-10), 31, 62, 3, 300);
        await ProcAsync(store, ServerIdB, "EdgeOnlyDb", "usp_RightEdge", h2.AddMinutes(5), 41, 82, 4, 300);
        await ProcAsync(store, ServerIdA, "PreUpgradeDb", "usp_NullInterval", h1.AddHours(7).AddMinutes(12), 4242, 8484, 6, null);
        await ProcAsync(store, ServerIdA, "MainDb", "usp_One", h1.AddMinutes(-10), 13, 26, 1, 300);
        await ProcAsync(store, ServerIdA, "MainDb", "usp_One", h2.AddMinutes(5), 17, 34, 2, 300);

        await using var refresh = new NpgsqlCommand(
            $"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.ProcedureStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", store.Connection);
        refresh.Parameters.AddWithValue(DateTime.SpecifyKind(h1.AddHours(-2), DateTimeKind.Unspecified));
        refresh.Parameters.AddWithValue(DateTime.SpecifyKind(h2, DateTimeKind.Unspecified));
        await refresh.ExecuteNonQueryAsync(store.Ct);
    }

    private static string Key(int serverId, string database, string objectName) => $"{serverId}|{database}|dbo|{objectName}";

    private static async Task<Dictionary<string, decimal?>> ReadAsync(Store store, ComposeAggregate aggregate, string column, bool hybrid, bool byHour)
    {
        var fn = aggregate switch { ComposeAggregate.Max => "MAX", ComposeAggregate.Min => "MIN", _ => "SUM" };
        var from = hybrid
            ? ComposeCompiler.HourlyRawEdgesRelation("procedure_stats", TimescaleSupport.ProcedureStatsIntervalHourlyView, aggregate, "$1", "$2", "$3", "$4")
            : "collect.procedure_stats";
        var hour = byHour ? "date_trunc('hour', f.collection_time)" : "NULL::timestamp";
        var sql = $"SELECT f.server_id, f.database_name, f.schema_name, f.object_name, {hour}, "
            + $"CAST({fn}(f.{column}) FILTER (WHERE f.sample_interval_seconds IS DISTINCT FROM 0) AS numeric) "
            + $"FROM {from} AS f WHERE f.collection_time >= $1 AND f.collection_time <= $2 "
            + $"GROUP BY f.server_id, f.database_name, f.schema_name, f.object_name{(byHour ? ", 5" : "")}";
        await using var command = new NpgsqlCommand(sql, store.Connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(store.WindowStart, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(store.WindowEnd, DateTimeKind.Unspecified));
        if (hybrid)
        {
            command.Parameters.AddWithValue(DateTime.SpecifyKind(store.H1, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(DateTime.SpecifyKind(store.H2, DateTimeKind.Unspecified));
        }

        var rows = new Dictionary<string, decimal?>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(store.Ct);
        while (await reader.ReadAsync(store.Ct))
        {
            var key = Key(reader.GetInt32(0), reader.GetString(1), reader.GetString(3))
                + (reader.IsDBNull(4) ? "" : "|" + reader.GetDateTime(4).ToString("o", CultureInfo.InvariantCulture));
            rows.Add(key, reader.IsDBNull(5) ? null : reader.GetDecimal(5));
        }

        return rows;
    }

    /// <summary>Every raw group with a value is in the arm-less read with that value, and the arm-less read holds no group raw lacks.
    /// A raw group with a NULL value may be absent from the arm-less read.</summary>
    private static void AssertSameValues(Dictionary<string, decimal?> raw, Dictionary<string, decimal?> armless, string what)
    {
        foreach (var (key, value) in raw)
        {
            if (value is null)
            {
                continue;
            }

            Assert.True(armless.TryGetValue(key, out var other), $"{what}: group {key} is missing from the arm-less read");
            Assert.Equal(value, other);
        }

        foreach (var (key, value) in armless)
        {
            Assert.True(raw.ContainsKey(key), $"{what}: group {key} is not in the raw read");
            Assert.Equal(raw[key], value);
        }
    }

    private static async Task ProcAsync(Store store, int serverId, string database, string objectName, DateTime at, long worker, long elapsed, long executions, int? interval)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, $8, $9, $10, $11)", store.Connection);
        insert.Parameters.AddWithValue(Interlocked.Increment(ref s_collectionId));
        insert.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(serverId == ServerIdA ? ServerA : ServerB);
        insert.Parameters.AddWithValue(database);
        insert.Parameters.AddWithValue(objectName);
        insert.Parameters.AddWithValue("0x" + objectName);
        insert.Parameters.AddWithValue(worker);
        insert.Parameters.AddWithValue(elapsed);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(interval is int value ? value : DBNull.Value);
        await insert.ExecuteNonQueryAsync(store.Ct);
    }

    private static async Task<Store> StoreAsync()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the #9357 procedure_stats rollup live test.");
        var ct = TestContext.Current.CancellationToken;

        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        NpgsqlConnection? connection = null;
        try
        {
            connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
            Assert.SkipWhen(!timescaleEnabled, "The hourly rollup is a continuous aggregate: TimescaleDB is required.");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerA, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerB, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            return new Store(scratch, connection, ct);
        }
        catch
        {
            if (connection is not null)
            {
                await connection.DisposeAsync();
            }

            await scratch.DisposeAsync();
            throw;
        }
    }

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        public Store(ScratchPostgres scratch, NpgsqlConnection connection, CancellationToken ct)
        {
            _scratch = scratch;
            Connection = connection;
            Ct = ct;
            WindowEnd = Anchor;
            WindowStart = Anchor.AddHours(-24);
            H2 = new DateTime(Anchor.Year, Anchor.Month, Anchor.Day, Anchor.Hour, 0, 0, DateTimeKind.Utc);
            H1 = new DateTime(WindowStart.Year, WindowStart.Month, WindowStart.Day, WindowStart.Hour, 0, 0, DateTimeKind.Utc).AddHours(1);
        }

        public NpgsqlConnection Connection { get; }
        public CancellationToken Ct { get; }
        public DateTime WindowStart { get; }
        public DateTime WindowEnd { get; }
        public DateTime H1 { get; }
        public DateTime H2 { get; }

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
