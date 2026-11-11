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
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing
   shared, so it needs no place in the live-postgres collection. */

/// <summary>
/// #8567: the sweep's purge of the rows it dropped unfetched (<see cref="OversizedPlanBacklog.PurgeDroppedSql"/>) reads an
/// index that holds those rows alone, so a pass reads the rows it deletes and not every row the server has waiting. The
/// index is rung V258 and the viewer probe's sentinel for it. Every time here is a fixed instant passed as a parameter.
/// </summary>
public sealed class OversizedPlanBacklogDroppedIndexLivePostgresTests
{
    private const int RungVersion = 258;

    private const string IndexName = "idx_oversized_plan_backlog_dropped";

    private const int ServerId = 4311;
    private const int OtherServerId = 4312;

    private const int Waiting = 40000;
    private const int DroppedBeforeTheCutoff = 40;
    private const int DroppedAfterTheCutoff = 60;
    private const int FetchExpired = 25;
    private const int OtherServerWaiting = 2000;
    private const int OtherServerDropped = 30;

    /// <summary>The purge's cutoff: rows dropped before it leave.</summary>
    private static readonly DateTime Cutoff = new(2026, 6, 1, 0, 0, 0, DateTimeKind.Unspecified);

    private const string ServersSql = @"
INSERT INTO collect.servers (server_id, server_name, is_enabled)
VALUES (4311, 'backlog-index-server', TRUE), (4312, 'backlog-index-other-server', TRUE);";

    /// <summary>$6 rows for server $1, keyed by the label $2, sighted at $3, with the given expiry ($4) and attempt ($5) stamps.</summary>
    private const string SeedSql = @"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at, expired_at, last_attempt_at)
SELECT $1, 'query_stats', $2 || '_PLAN_' || n, $2 || '_SQL_' || n, 0, 400, 'index_db', $2 || '_HASH_' || n, 600000 + n, $3, $3, $4, $5
FROM generate_series(1, $6) AS n;";

    private const string IndexSql = @"
SELECT
    i.indisvalid,
    i.indisunique,
    i.indpred IS NOT NULL,
    ARRAY
    (
        SELECT a.attname::text
        FROM unnest(i.indkey::smallint[]) WITH ORDINALITY AS k (attnum, ordinal)
        JOIN pg_attribute AS a
          ON  a.attrelid = i.indrelid
          AND a.attnum = k.attnum
        WHERE k.ordinal <= i.indnkeyatts
        ORDER BY k.ordinal
    )
FROM pg_index AS i
JOIN pg_class AS c
  ON c.oid = i.indexrelid
WHERE i.indrelid = 'collect.oversized_plan_backlog'::regclass
AND   c.relname = $1;";

    private const string RowsSql = @"
SELECT count(*)
FROM collect.oversized_plan_backlog
WHERE server_id = $1;";

    private sealed record IndexShape(bool IsValid, bool IsUnique, bool IsPartial, string[] KeyColumns);

    /// <summary>
    /// A store the ladder migrated has the index: on the server and then the drop stamp, and partial, so a waiting row is
    /// not in it. The index is the probe's sentinel for its rung: with it the probe names the rung, without it the rung
    /// below, and running the rung again makes it again.
    /// </summary>
    [Fact]
    public async Task AMigratedStore_HasTheDroppedRowIndex_AndTheProbeNamesItsRung()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8567 backlog index test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = await MigratedConnectionAsync(scratch, ct);

        var index = await ReadIndexAsync(connection, ct);
        Assert.True(index is not null, $"collect.oversized_plan_backlog has no index named {IndexName} on a store the ladder migrated");
        Assert.Equal(new[] { "server_id", "expired_at" }, index!.KeyColumns);
        Assert.True(index.IsPartial, "the index holds every row of the table, the waiting ones included");
        Assert.True(index.IsValid);
        Assert.False(index.IsUnique);

        /* The probe maps the newest sentinel present, so the sentinels of any rung above this one go first. */
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion, ct);
        Assert.Equal(RungVersion, await ProbedVersionAsync(connection, ct));

        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
        Assert.Null(await ReadIndexAsync(connection, ct));
        Assert.Equal(RungVersion - 1, await ProbedVersionAsync(connection, ct));

        var rung = PgMigrations.Scripts.Single(m => m.Version == RungVersion).Sql;
        for (var run = 0; run < 2; run++)
        {
            await using var again = new NpgsqlCommand(rung, connection);
            await again.ExecuteNonQueryAsync(ct);
        }

        Assert.NotNull(await ReadIndexAsync(connection, ct));
        Assert.Equal(RungVersion, await ProbedVersionAsync(connection, ct));
    }

    /// <summary>
    /// One server has forty thousand rows waiting and a hundred dropped unfetched, forty of them before the cutoff. The
    /// purge, bound as the sweep binds it, reads the dropped-row index and nothing else: no scan of the table and no walk
    /// of the server's rows through the primary key. It removes the forty and leaves the waiting rows, the rows dropped
    /// after the cutoff, the rows a fetch expired and the other server's rows.
    /// </summary>
    [Fact]
    public async Task ThePurgeOfDroppedRows_ReadsTheDroppedRowIndex_AndNotTheServersWaitingRows()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8567 backlog index test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = await MigratedConnectionAsync(scratch, ct);

        await using (var servers = new NpgsqlCommand(ServersSql, connection))
        {
            await servers.ExecuteNonQueryAsync(ct);
        }

        var sighted = Cutoff.AddDays(-3);
        var before = Cutoff.AddDays(-1);
        var after = Cutoff.AddHours(1);
        await SeedAsync(connection, ServerId, "0xWAITING", sighted, null, null, Waiting, ct);
        await SeedAsync(connection, ServerId, "0xDROPPED_BEFORE", sighted, before, null, DroppedBeforeTheCutoff, ct);
        await SeedAsync(connection, ServerId, "0xDROPPED_AFTER", sighted, after, null, DroppedAfterTheCutoff, ct);
        await SeedAsync(connection, ServerId, "0xFETCH_EXPIRED", sighted, before, before, FetchExpired, ct);
        await SeedAsync(connection, OtherServerId, "0xWAITING", sighted, null, null, OtherServerWaiting, ct);
        await SeedAsync(connection, OtherServerId, "0xDROPPED_BEFORE", sighted, before, null, OtherServerDropped, ct);

        await using (var analyze = new NpgsqlCommand("ANALYZE collect.oversized_plan_backlog;", connection))
        {
            await analyze.ExecuteNonQueryAsync(ct);
        }

        string plan;
        await using (var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + OversizedPlanBacklog.PurgeDroppedSql, connection))
        {
            BindPurge(explain);
            plan = (string)(await explain.ExecuteScalarAsync(ct))!;
        }

        var nodeTypes = new List<string>();
        var indexes = new List<string>();
        using (var document = JsonDocument.Parse(plan))
        {
            Walk(document.RootElement[0].GetProperty("Plan"), nodeTypes, indexes);
        }

        Assert.True(indexes.Contains(IndexName), $"the purge does not read {IndexName}: " + plan);
        Assert.True(indexes.All(name => name == IndexName), "the purge also reads another index: " + plan);
        Assert.DoesNotContain("Seq Scan", nodeTypes);

        await using (var purge = new NpgsqlCommand(OversizedPlanBacklog.PurgeDroppedSql, connection))
        {
            BindPurge(purge);
            Assert.Equal(DroppedBeforeTheCutoff, await purge.ExecuteNonQueryAsync(ct));
        }

        Assert.Equal(Waiting + DroppedAfterTheCutoff + FetchExpired, await RowsAsync(connection, ServerId, ct));
        Assert.Equal(OtherServerWaiting + OtherServerDropped, await RowsAsync(connection, OtherServerId, ct));
    }

    /// <summary>The purge's parameters as <c>OversizedPlanBacklogSweep</c> binds them: the server, then the cutoff.</summary>
    private static void BindPurge(NpgsqlCommand command)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Cutoff });
    }

    private static void Walk(JsonElement node, List<string> nodeTypes, List<string> indexes)
    {
        nodeTypes.Add(node.GetProperty("Node Type").GetString()!);
        if (node.TryGetProperty("Index Name", out var index))
        {
            indexes.Add(index.GetString()!);
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Walk(child, nodeTypes, indexes);
            }
        }
    }

    /// <summary>Runs the ladder over the scratch database, then opens a connection on the store's search path.</summary>
    private static async Task<NpgsqlConnection> MigratedConnectionAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
        }

        var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    private static async Task SeedAsync(
        NpgsqlConnection connection, int serverId, string label, DateTime sightedAt, DateTime? expiredAt, DateTime? lastAttemptAt, int rows,
        CancellationToken ct)
    {
        await using var seed = new NpgsqlCommand(SeedSql, connection);
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = label });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = sightedAt });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = (object?)expiredAt ?? DBNull.Value });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = (object?)lastAttemptAt ?? DBNull.Value });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = rows });
        Assert.Equal(rows, await seed.ExecuteNonQueryAsync(ct));
    }

    private static async Task<IndexShape?> ReadIndexAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(IndexSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = IndexName });
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        return new IndexShape(reader.GetBoolean(0), reader.GetBoolean(1), reader.GetBoolean(2), reader.GetFieldValue<string[]>(3));
    }

    private static async Task<long> RowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(RowsSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The schema version the viewer's probe reads from this store.</summary>
    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var map = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var sentinels = new object[map.GetParameters().Length];

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal(sentinels.Length, reader.FieldCount);
        for (var i = 0; i < sentinels.Length; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)map.Invoke(null, sentinels)!;
    }
}
