/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live, end-to-end proof of the #4348 one-time scrub against real rows in both target tables, including a
/// compressed <c>pg_blocking_edges</c> chunk.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database (<see cref="ScratchPostgres"/>) rather than
/// sharing the live fixture, the same reason <c>PgSettingScrubLiveTests</c> does: it creates its own
/// hypertable/compression shape on <c>collect.pg_blocking_edges</c>, which the shared fixture must never
/// inherit from a test.</para>
/// </summary>
public sealed class PgStatementTextScrubLiveTests
{
    private const int ServerA = -444501;
    private const int ServerB = -444502;
    private static readonly DateTime Day = DateTime.SpecifyKind(new DateTime(2026, 1, 1, 0, 0, 0), DateTimeKind.Unspecified);

    private const string Secret = "ALTER ROLE app PASSWORD 'secret-x'";
    private const string Neighbor = "SELECT * FROM t WHERE password_changed_at > $1";

    [Fact]
    public async Task TheScrubReplacesMatchingRows_LeavesNeighborsUntouched_SetsTheMarker_AndTheSecondRunChangesNothing()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4348 statement-text scrub (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using (var setupConnection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setupConnection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setupConnection, ct);

            Assert.True(await TimescaleSupport.TryEnableAsync(setupConnection, null, ct),
                "the dev fixture is expected to have TimescaleDB installed");
            await ExecAsync(setupConnection, "SELECT create_hypertable('collect.pg_blocking_edges', by_range('collection_time', INTERVAL '1 days'), if_not_exists => true)", ct);
            await ExecAsync(setupConnection, "ALTER TABLE collect.pg_blocking_edges SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", ct);

            /* collect.pg_statement_text — matching + neighbor row, for two servers. */
            await InsertStatementTextAsync(setupConnection, ServerA, 1001, Secret, ct);
            await InsertStatementTextAsync(setupConnection, ServerA, 1002, Neighbor, ct);
            await InsertStatementTextAsync(setupConnection, ServerB, 2001, Secret, ct);
            await InsertStatementTextAsync(setupConnection, ServerB, 2002, Neighbor, ct);

            /* collect.pg_blocking_edges — matching + neighbor row, for two servers, same day, same chunk. */
            await InsertBlockingEdgeAsync(setupConnection, ServerA, Day, 1, Secret, Neighbor, ct);
            await InsertBlockingEdgeAsync(setupConnection, ServerA, Day, 2, Neighbor, Neighbor, ct);
            await InsertBlockingEdgeAsync(setupConnection, ServerB, Day, 3, Secret, Neighbor, ct);
            await InsertBlockingEdgeAsync(setupConnection, ServerB, Day, 4, Neighbor, Neighbor, ct);

            await ExecAsync(setupConnection, "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.pg_blocking_edges') c", ct);

            Assert.True(await ContainsSecretAsync(setupConnection, ct), "seeding failed to plant the secret this test exists to catch");
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var first = await PgStatementTextScrub.RunAsync(postgres, logger: null, ct);
        Assert.False(first.AlreadyDone);
        Assert.Equal(2, first.StatementTextRowsUpdated);
        Assert.Equal(2, first.BlockingEdgesRowsUpdated);

        await using var verifyConnection = new NpgsqlConnection(scratch.ConnectionString);
        await verifyConnection.OpenAsync(ct);

        Assert.False(await ContainsSecretAsync(verifyConnection, ct), "the raw statement text survived the scrub");

        /* Matching rows became the placeholder. */
        Assert.Equal(PerformanceMonitor.Collectors.PgSensitiveStatementFilter.PlaceholderText,
            await ScalarTextAsync(verifyConnection, "SELECT query_text FROM collect.pg_statement_text WHERE server_id = $1 AND queryid = $2", ServerA, 1001, ct));
        Assert.Equal(PerformanceMonitor.Collectors.PgSensitiveStatementFilter.PlaceholderText,
            await ScalarTextAsync(verifyConnection, "SELECT query_text FROM collect.pg_statement_text WHERE server_id = $1 AND queryid = $2", ServerB, 2001, ct));
        Assert.Equal(PerformanceMonitor.Collectors.PgSensitiveStatementFilter.PlaceholderText,
            await ScalarTextAsync(verifyConnection, "SELECT blocked_query FROM collect.pg_blocking_edges WHERE server_id = $1 AND collection_id = $2", ServerA, 1, ct));
        Assert.Equal(PerformanceMonitor.Collectors.PgSensitiveStatementFilter.PlaceholderText,
            await ScalarTextAsync(verifyConnection, "SELECT blocked_query FROM collect.pg_blocking_edges WHERE server_id = $1 AND collection_id = $2", ServerB, 3, ct));

        /* Neighbor rows are byte-identical. */
        Assert.Equal(Neighbor,
            await ScalarTextAsync(verifyConnection, "SELECT query_text FROM collect.pg_statement_text WHERE server_id = $1 AND queryid = $2", ServerA, 1002, ct));
        Assert.Equal(Neighbor,
            await ScalarTextAsync(verifyConnection, "SELECT query_text FROM collect.pg_statement_text WHERE server_id = $1 AND queryid = $2", ServerB, 2002, ct));
        Assert.Equal(Neighbor,
            await ScalarTextAsync(verifyConnection, "SELECT blocked_query FROM collect.pg_blocking_edges WHERE server_id = $1 AND collection_id = $2", ServerA, 2, ct));
        Assert.Equal(Neighbor,
            await ScalarTextAsync(verifyConnection, "SELECT blocking_query FROM collect.pg_blocking_edges WHERE server_id = $1 AND collection_id = $2", ServerA, 1, ct));

        /* The marker equals ScrubVersion. */
        var markerValue = await ScalarTextAsync(
            verifyConnection,
            "SELECT state_value FROM collect.collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3",
            DarlingObservability.FleetServerId, PgStatementTextScrub.StateCollectorName, PgStatementTextScrub.ScrubVersionStateKey, ct);
        Assert.Equal(PgStatementTextScrub.ScrubVersion.ToString(CultureInfo.InvariantCulture), markerValue);

        /* A second run changes 0 rows. */
        var second = await PgStatementTextScrub.RunAsync(postgres, logger: null, ct);
        Assert.True(second.AlreadyDone);
        Assert.Equal(0, second.StatementTextRowsUpdated);
        Assert.Equal(0, second.BlockingEdgesRowsUpdated);
    }

    private static async Task<bool> ContainsSecretAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var countCommand = new NpgsqlCommand(
            "SELECT count(*) FROM collect.pg_statement_text WHERE query_text = $1", connection);
        countCommand.Parameters.AddWithValue(Secret);
        var statementCount = (long)(await countCommand.ExecuteScalarAsync(ct))!;
        await countCommand.DisposeAsync();

        var edgesCommand = new NpgsqlCommand(
            "SELECT count(*) FROM collect.pg_blocking_edges WHERE blocked_query = $1 OR blocking_query = $1", connection);
        edgesCommand.Parameters.AddWithValue(Secret);
        var edgesCount = (long)(await edgesCommand.ExecuteScalarAsync(ct))!;
        await edgesCommand.DisposeAsync();

        return statementCount > 0 || edgesCount > 0;
    }

    private static async Task<string?> ScalarTextAsync(
        NpgsqlConnection connection, string sql, int serverId, long key, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = key });
        var result = await command.ExecuteScalarAsync(ct);
        return result as string;
    }

    private static async Task<string?> ScalarTextAsync(
        NpgsqlConnection connection, string sql, int serverId, string collectorName, string stateKey, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = collectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = stateKey });
        var result = await command.ExecuteScalarAsync(ct);
        return result as string;
    }

    private static async Task InsertStatementTextAsync(
        NpgsqlConnection connection, int serverId, long queryId, string queryText, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.pg_statement_text (server_id, queryid, query_text, first_seen, last_seen) " +
            "VALUES ($1, $2, $3, now()::timestamp, now()::timestamp)", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = queryText });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertBlockingEdgeAsync(
        NpgsqlConnection connection, int serverId, DateTime day, long collectionId, string blockedQuery, string blockingQuery,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.pg_blocking_edges (
    collection_id, collection_time, server_id, server_name, blocked_query, blocking_query)
VALUES ($1, $2, $3, $4, $5, $6)", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = collectionId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = day });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = "server-" + serverId.ToString(CultureInfo.InvariantCulture) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = blockedQuery });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = blockingQuery });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
