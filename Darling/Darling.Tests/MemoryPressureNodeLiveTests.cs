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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7691: the memory node on the memory pressure events, against a real PostgreSQL store. V250 adds
/// <c>collect.memory_pressure_events.memory_node_id</c> and re-expands <c>v_memory_pressure_events</c>; the collector's own write path
/// stores the node; the Darling reader returns it. Gated on DARLING_TEST_PG, each test on its own scratch database.
/// </summary>
/* #1776 own-store: every test builds its own scratch database (ScratchPostgres) and drops it. */
[Collection("live-postgres")]
public sealed class MemoryPressureNodeLiveTests
{
    private const int TestServerId = -769110;
    private const string TestServerName = "memory-pressure-node-test";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>
    /// A store at V249 has no <c>memory_node_id</c> and a view over the older shape. Built the way the rewind does it (the view dropped, the
    /// column dropped, the view created again), with a row planted, then the V250 stamp is rolled back and the ladder re-run: V250 and every
    /// rung above it is left to apply. The column must exist as a nullable integer with no default, the view must return it (NULL for the old row), and a
    /// second run must change nothing.
    /// </summary>
    [Fact]
    public async Task AStoreAtV249_TakesV250_TheColumnIsNullableInteger_AndTheViewReturnsIt()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the V250 live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await ExecAsync(connection, "DROP VIEW collect.v_memory_pressure_events", ct);
        await ExecAsync(connection, "ALTER TABLE collect.memory_pressure_events DROP COLUMN memory_node_id", ct);
        await ExecAsync(connection, "CREATE VIEW collect.v_memory_pressure_events AS SELECT * FROM collect.memory_pressure_events", ct);
        await ExecAsync(connection,
            "INSERT INTO collect.memory_pressure_events (collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system) "
            + $"VALUES (1, '2026-01-01 00:00:00', {TestServerId}, '{TestServerName}', '2026-01-01 00:00:00', 'RESOURCE_MEMPHYSICAL_LOW', 2, 0)", ct);

        Assert.Equal(false, await ScalarAsync(connection,
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'v_memory_pressure_events' AND column_name = 'memory_node_id')", ct));

        await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 250", ct);
        /* V250 and every rung above it is left to apply (V251 since #7762), so the count is read from the ladder. */
        Assert.Equal(PgMigrations.Scripts.Count(s => s.Version >= 250), await PgMigrations.MigrateAsync(connection, ct));

        await using (var column = new NpgsqlCommand(
            "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
            + "WHERE table_schema = 'collect' AND table_name = 'memory_pressure_events' AND column_name = 'memory_node_id'", connection))
        await using (var reader = await column.ExecuteReaderAsync(ct))
        {
            Assert.True(await reader.ReadAsync(ct), "V250 did not add memory_node_id to collect.memory_pressure_events");
            Assert.Equal("integer", reader.GetString(0));
            Assert.Equal("YES", reader.GetString(1));
            Assert.True(reader.IsDBNull(2), "the column has no default");
        }

        Assert.Equal(true, await ScalarAsync(connection,
            "SELECT memory_node_id IS NULL FROM collect.v_memory_pressure_events WHERE collection_id = 1", ct));

        Assert.Equal(0, await PgMigrations.MigrateAsync(connection, ct));
    }

    /// <summary>
    /// Two records of one event on two memory nodes, with the same <c>sample_time</c>, written through the collector's own write path
    /// (<c>PgCollectorRowWriter</c> over <c>MemoryPressureEventsCollector.WritePayload</c>), then read back through the Darling reader. Both
    /// rows must be stored with their node, and the reader must return both with that node, plus a record that carries no node as NULL.
    /// </summary>
    [Fact]
    public async Task TwoRecordsAtOneTimestampOnTwoNodes_AreStoredWithTheirNode_AndTheReaderReturnsBoth()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the memory node write-path live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var definition = MemoryPressureEventsCollector.Instance;
        var runTime = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Unspecified);
        var sample = new DateTime(2026, 10, 12, 11, 30, 5, DateTimeKind.Unspecified);
        var rows = new[]
        {
            new MemoryPressureEventsCollector.Row(sample, "RESOURCE_MEMPHYSICAL_LOW", 2, 0, 0),
            new MemoryPressureEventsCollector.Row(sample, "RESOURCE_MEMPHYSICAL_LOW", 2, 0, 1),
            new MemoryPressureEventsCollector.Row(sample.AddMinutes(5), "RESOURCE_MEM_STEADY", 1, 0),
        };

        var writer = new PgCollectorRowWriter();
        await using (var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct))
        {
            writer.Importer = importer;
            foreach (var row in rows)
            {
                await importer.StartRowAsync(ct);
                writer.Value(CollectionIdGenerator.Next());
                writer.Value(runTime).Value(TestServerId).Value(TestServerName);
                writer.BeginPayload();
                definition.WritePayload(row, writer, new CollectorContext { ServerId = TestServerId, ServerName = TestServerName, CollectionTime = runTime, Deltas = null! });
                writer.EndPayload(definition.PayloadColumns.Count);
            }

            await importer.CompleteAsync(ct);
        }

        Assert.Equal("0,1,<null>", (string)(await ScalarAsync(connection,
            "SELECT string_agg(COALESCE(memory_node_id::text, '<null>'), ',' ORDER BY sample_time, memory_node_id NULLS LAST) FROM collect.memory_pressure_events WHERE server_id = " + TestServerId, ct))!);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var page = await DarlingMemoryGrantReader.GetMemoryPressureEventsAsync(
            postgres, TestServerId, sample.AddHours(-1), sample.AddHours(1), limit: null, ct);

        Assert.Equal(3, page.Rows.Count);
        var atSample = page.Rows.Where(r => r.SampleTime == sample).Select(r => r.MemoryNodeId).OrderBy(n => n).ToArray();
        Assert.Equal(new int?[] { 0, 1 }, atSample);
        Assert.Null(page.Rows.Single(r => r.SampleTime == sample.AddMinutes(5)).MemoryNodeId);
    }
}
