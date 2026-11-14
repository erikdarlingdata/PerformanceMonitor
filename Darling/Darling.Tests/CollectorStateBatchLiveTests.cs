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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #8795: one cycle's <c>collector_state</c> keys are written as one statement, which is one commit and one WAL flush, where each key used to be
/// its own statement. The rows stored, the conflict outcome and the <c>updated_at</c> convention must be what the per-key statements stored, so
/// every test writes the same cycle both ways, to two server ids of one store, and compares the rows. The per-key side is the statement the
/// service ran before, and it doubles as the control that proves the command counter sees this table's writes.
/// </summary>
public sealed class CollectorStateBatchLiveTests
{
    private const int PerKeyServerId = -879501;
    private const int BatchedServerId = -879502;
    private const string Collector = "query_store";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The statement <c>SaveCollectorStateAsync</c> ran once per key before #8795.</summary>
    private const string PerKeySql = @"
INSERT INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (server_id, collector_name, state_key)
DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at";

    private static async Task WritePerKeyAsync(NpgsqlDataSource source, int serverId, string collector, IReadOnlyDictionary<string, string> state, CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        foreach (var entry in state)
        {
            using var command = new NpgsqlCommand(PerKeySql, connection);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(collector);
            command.Parameters.AddWithValue(entry.Key);
            command.Parameters.AddWithValue(entry.Value);
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<List<(string Collector, string Key, string Value, DateTime UpdatedAt)>> ReadRowsAsync(NpgsqlDataSource source, int serverId, CancellationToken ct)
    {
        await using var connection = await source.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(
            "SELECT collector_name, state_key, state_value, updated_at FROM collect.collector_state WHERE server_id = $1 ORDER BY collector_name, state_key", connection);
        command.Parameters.AddWithValue(serverId);
        var rows = new List<(string, string, string, DateTime)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetDateTime(3)));
        return rows;
    }

    private static void AssertSameRows(
        List<(string Collector, string Key, string Value, DateTime UpdatedAt)> expected,
        List<(string Collector, string Key, string Value, DateTime UpdatedAt)> actual)
    {
        Assert.Equal(
            expected.Select(r => (r.Collector, r.Key, r.Value)).ToList(),
            actual.Select(r => (r.Collector, r.Key, r.Value)).ToList());

        /* NAIVE UTC like every other timestamp in the store: a Kind=Utc bind would land the row offset by the server's zone. */
        foreach (var row in actual)
        {
            Assert.Equal(DateTimeKind.Unspecified, row.UpdatedAt.Kind);
            Assert.True((DateTime.UtcNow - row.UpdatedAt).Duration() < TimeSpan.FromMinutes(5), $"updated_at must be naive UTC, stored {row.UpdatedAt:o}");
        }
    }

    [Fact]
    public async Task OneCycleWithManyKeys_StoresWhatPerKeyWritesStored_InOneStatement()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live collector-state batch test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(source, new CollectorDeltaCalculator());

        var first = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["qsowm:db_a"] = "2026-11-01T00:00:00",
            ["qsowm:db_b"] = "2026-11-01T00:05:00",
            ["qsowm:db_c"] = "2026-11-01T00:10:00",
            ["qsowm:db_d"] = "2026-11-01T00:15:00",
            ["qsowm:db_e"] = "2026-11-01T00:20:00",
        };

        long perKeyStatements;
        long batchedStatements;
        using (var counter = new NpgsqlCommandCounter(scratch.DatabaseName, "collector_state"))
        {
            await WritePerKeyAsync(source, PerKeyServerId, Collector, first, ct);
            perKeyStatements = counter.Count;
        }

        /* The control: the counter really sees this table's writes, one per key. A renamed Npgsql tag reads as a loud failure here. */
        Assert.Equal(first.Count, perKeyStatements);

        using (var counter = new NpgsqlCommandCounter(scratch.DatabaseName, "collector_state"))
        {
            await runner.SaveCollectorStateAsync(BatchedServerId, Collector, first, ct);
            batchedStatements = counter.Count;
        }

        Assert.Equal(1, batchedStatements);
        AssertSameRows(await ReadRowsAsync(source, PerKeyServerId, ct), await ReadRowsAsync(source, BatchedServerId, ct));

        /* The conflict outcome: three keys move, one is new, one is untouched. Same rows, same count, no duplicates. */
        var second = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["qsowm:db_a"] = "2026-11-02T00:00:00",
            ["qsowm:db_c"] = "2026-11-02T00:10:00",
            ["qsowm:db_e"] = "2026-11-02T00:20:00",
            ["qsowm:db_f"] = "2026-11-02T00:25:00",
        };
        await WritePerKeyAsync(source, PerKeyServerId, Collector, second, ct);
        await runner.SaveCollectorStateAsync(BatchedServerId, Collector, second, ct);

        var expected = await ReadRowsAsync(source, PerKeyServerId, ct);
        var actual = await ReadRowsAsync(source, BatchedServerId, ct);
        Assert.Equal(6, actual.Count);
        AssertSameRows(expected, actual);
        Assert.Equal("2026-11-02T00:10:00", actual.Single(r => r.Key == "qsowm:db_c").Value);
        Assert.Equal("2026-11-01T00:05:00", actual.Single(r => r.Key == "qsowm:db_b").Value);

        /* An empty cycle writes nothing. */
        using (var counter = new NpgsqlCommandCounter(scratch.DatabaseName, "collector_state"))
        {
            await runner.SaveCollectorStateAsync(BatchedServerId, Collector, new Dictionary<string, string>(), ct);
            Assert.Equal(0, counter.Count);
        }
    }

    [Fact]
    public async Task OneCycleWithTwoOwners_StoresWhatTwoOwnerWritesStored_InOneStatement()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live collector-state batch test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(source, new CollectorDeltaCalculator());

        var stamps = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["qsowm:db_a"] = "2026-11-01T00:00:00",
            ["qsowm:db_b"] = "2026-11-01T00:05:00",
        };
        var others = new Dictionary<string, string>(StringComparer.Ordinal) { ["last_run"] = "2026-11-01T00:06:00" };
        var stampOwner = QueryStoreOpenIntervalState.StateCollectorName;

        /* What the cycle did before #8795: one call per owner. */
        await runner.SaveCollectorStateAsync(PerKeyServerId, stampOwner, stamps, ct);
        await runner.SaveCollectorStateAsync(PerKeyServerId, Collector, others, ct);

        long statements;
        using (var counter = new NpgsqlCommandCounter(scratch.DatabaseName, "collector_state"))
        {
            await runner.SaveCollectorStateRowsAsync(
                BatchedServerId,
                [(stampOwner, "qsowm:db_a", stamps["qsowm:db_a"]), (stampOwner, "qsowm:db_b", stamps["qsowm:db_b"]), (Collector, "last_run", others["last_run"])],
                ct);
            statements = counter.Count;
        }

        Assert.Equal(1, statements);
        var expected = await ReadRowsAsync(source, PerKeyServerId, ct);
        Assert.Equal(3, expected.Count);
        Assert.Equal(2, expected.Select(r => r.Collector).Distinct().Count());
        AssertSameRows(expected, await ReadRowsAsync(source, BatchedServerId, ct));

        /* A second call over the same keys is an upsert, not a duplicate-key failure. */
        await runner.SaveCollectorStateRowsAsync(BatchedServerId, [(Collector, "last_run", "2026-11-02T00:06:00")], ct);
        var after = await ReadRowsAsync(source, BatchedServerId, ct);
        Assert.Equal(3, after.Count);
        Assert.Equal("2026-11-02T00:06:00", after.Single(r => r.Key == "last_run").Value);
    }
}
