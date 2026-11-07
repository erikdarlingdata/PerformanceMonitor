/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8092: on an Amazon Aurora target the write side of <c>pg_stat_io</c> is not measured, but the engine does not always
/// say so with NULL: an Aurora PostgreSQL 16 target reports <c>writes = 0</c>. Derived from <c>writes IS NOT NULL</c>
/// alone, <c>write_counters_tracked</c> then read true, the payload carried <c>writes: 0</c> as a measurement, and the
/// Aurora sentence both I/O reads hold ready never fired, while their descriptions promised null. The readers now gate
/// the flag on the registry's engine kind as well, so the zeros read as unmeasured wherever the row came from.
/// </summary>
[Collection("live-postgres")]
public sealed class PgIoAuroraWriteCountersLiveTests
{
    private const int ServerId = -8092201;
    private const string ServerName = "pg-io-aurora-write-counters";

    private static readonly DateTime WindowStart = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(1);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheIoRead_OnAurora_ReportsTheZeroWriteCountersAsUntracked_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the Aurora write-counter test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct, MonitoredEngineKind.AuroraPostgres);

        await using var dataSource = StoreDataSource(scratch);
        var page = await DarlingPgIoReader.GetPgIoPageAsync(dataSource, ServerId, WindowStart, WindowEnd, 20, ct);

        Assert.NotEmpty(page.Rows);
        Assert.All(page.Rows, r => Assert.False(r.WriteCountersTracked, $"{r.BackendType}/{r.ObjectType}/{r.Context} reads its zero writes as tracked on an Aurora target"));
    }

    [Fact]
    public async Task TheIoTool_OnAurora_NullsTheWriteFields_AndNamesAurora_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the Aurora write-counter test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct, MonitoredEngineKind.AuroraPostgres);

        await using var dataSource = StoreDataSource(scratch);
        var json = await DarlingMcpPgIoTools.GetPgIoStats(
            dataSource, ServerName, hours_back: 1, limit: 20, as_of: WebDataStartNote.FormatWindowEnd(WindowEnd), cancellationToken: ct);

        Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal("io_activity", root.GetProperty("status").GetString());
        Assert.False(root.GetProperty("write_counters_tracked_anywhere").GetBoolean());
        Assert.Contains("Amazon Aurora", root.GetProperty("note").GetString(), StringComparison.Ordinal);

        var combinations = root.GetProperty("combinations").EnumerateArray().ToList();
        Assert.NotEmpty(combinations);
        Assert.All(combinations, c =>
        {
            Assert.False(c.GetProperty("write_counters_tracked").GetBoolean());
            Assert.Equal(JsonValueKind.Null, c.GetProperty("writes").ValueKind);
            Assert.Equal(JsonValueKind.Null, c.GetProperty("write_bytes").ValueKind);
        });
    }

    [Fact]
    public async Task TheIoTrendTool_OnAurora_NullsTheWriteFields_AndNamesAurora_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the Aurora write-counter test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct, MonitoredEngineKind.AuroraPostgres);

        await using var dataSource = StoreDataSource(scratch);
        var json = await DarlingMcpPgTrendTools.GetPgIoTrend(
            dataSource, ServerName, backend_type: "client backend", context: "normal", hours_back: 1,
            as_of: WebDataStartNote.FormatWindowEnd(WindowEnd), cancellationToken: ct);

        Assert.False(McpHelpers.IsErrorEnvelope(json), $"tool returned an error: {json}");
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal("io_trend", root.GetProperty("status").GetString());
        Assert.False(root.GetProperty("write_counters_tracked").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("total_writes").ValueKind);
        Assert.Contains("Amazon Aurora", root.GetProperty("note").GetString(), StringComparison.Ordinal);

        var points = root.GetProperty("points").EnumerateArray().ToList();
        Assert.NotEmpty(points);
        Assert.All(points, p => Assert.Equal(JsonValueKind.Null, p.GetProperty("writes_per_second").ValueKind));
    }

    /// <summary>
    /// The same zeros on a stock engine are a measurement: a server that wrote nothing in the window. The engine gate
    /// must not turn every idle write side into "unmeasured".
    /// </summary>
    [Fact]
    public async Task TheSameZeros_OnAStockEngine_StayTracked_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the Aurora write-counter test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = await OpenStoreAsync(scratch, ct);
        await SeedAsync(connection, ct, MonitoredEngineKind.Postgres);

        await using var dataSource = StoreDataSource(scratch);
        var page = await DarlingPgIoReader.GetPgIoPageAsync(dataSource, ServerId, WindowStart, WindowEnd, 20, ct);
        Assert.NotEmpty(page.Rows);
        Assert.All(page.Rows, r => Assert.True(r.WriteCountersTracked));

        var stats = await DarlingMcpPgIoTools.GetPgIoStats(
            dataSource, ServerName, hours_back: 1, limit: 20, as_of: WebDataStartNote.FormatWindowEnd(WindowEnd), cancellationToken: ct);
        Assert.False(McpHelpers.IsErrorEnvelope(stats), $"tool returned an error: {stats}");
        using (var doc = JsonDocument.Parse(stats))
        {
            Assert.True(doc.RootElement.GetProperty("write_counters_tracked_anywhere").GetBoolean());
            var first = doc.RootElement.GetProperty("combinations").EnumerateArray().First();
            Assert.Equal(0, first.GetProperty("writes").GetInt64());
        }

        var trend = await DarlingMcpPgTrendTools.GetPgIoTrend(
            dataSource, ServerName, backend_type: "client backend", context: "normal", hours_back: 1,
            as_of: WebDataStartNote.FormatWindowEnd(WindowEnd), cancellationToken: ct);
        Assert.False(McpHelpers.IsErrorEnvelope(trend), $"tool returned an error: {trend}");
        using (var doc = JsonDocument.Parse(trend))
        {
            Assert.True(doc.RootElement.GetProperty("write_counters_tracked").GetBoolean());
            Assert.Equal(0, doc.RootElement.GetProperty("total_writes").GetInt64());
        }
    }

    /// <summary>
    /// What the engine reports on the target in the issue: three snapshots of one combination, reads climbing, the whole
    /// write side 0 rather than NULL, no byte columns (pre-18). The registry row carries the engine kind under test.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, string engineKind)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE servers SET engine_kind = $1, postgres_major_version = 16 WHERE server_id = $2", engineKind, ServerId);

        for (var s = 0; s < 3; s++)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_io_stats (collection_id, collection_time, server_id, server_name, backend_type, object_type, context,
    reads, read_time_ms, writes, write_time_ms, writebacks, writeback_time_ms, extends, extend_time_ms, op_bytes,
    hits, evictions, reuses, fsyncs, fsync_time_ms, stats_reset, read_bytes, write_bytes, extend_bytes)
VALUES ($1, $2, $3, $4, 'client backend', 'relation', 'normal',
    $5, 0.0, 0, 0.0, 0, 0.0, 0, 0.0, 8192,
    $6, 0, 0, 0, 0.0, NULL, NULL, NULL, NULL)",
                (long)(s + 1), DarlingMcpTestData.Naive(WindowStart.AddMinutes(5 + s * 5)), ServerId, ServerName,
                (long)(100 * (s + 1)), (long)(1_000 * (s + 1)));
        }
    }

    /// <summary>
    /// A seed session on the scratch store with the store's <c>search_path</c> SET, never inherited (the #8013 trap): a
    /// cloned scratch database does not carry the template's database-default <c>search_path</c>, so on the bare table
    /// names the seeds use resolve to nothing (42P01) without it.
    /// </summary>
    private static async Task<NpgsqlConnection> OpenStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    private static NpgsqlDataSource StoreDataSource(ScratchPostgres scratch) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath });
}
