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
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The baseline marker read names the collectors that can write an epoch label (#8570). It must return what the read
/// before it returned: every <c>collection_log</c> row of the server in the window whose note carries one of the three
/// labels, oldest first. The reference here is that earlier read, kept as text, run against the same store and the same
/// window. The store holds one <c>collection_log</c> row per collector per interval for three servers across four days,
/// as the store keeps it: day-sized chunks, the chunks older than a day compressed, so the markers sit in compressed
/// and in plain chunks. Each of the four collectors that observe an epoch carries markers, with host notes in front of
/// some, beside rows whose notes only look like a label.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The live fact reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it, so it cannot race live
   collection. */
public sealed class BaselineMarkerReadLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the baseline marker read's live comparison (it mints its own scratch database).";
    private const int Servers = 3;
    private const int Days = 4;
    private const int MarkersPerWriter = 3;
    private const int Writers = 4;

    private static readonly DateTime Start = new(2026, 1, 10, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The read as it was before it named the collectors: no collector filter, every row of the server in the window whose
    /// note carries a label.</summary>
    private const string ReferenceSql =
        "SELECT collection_time, collector_name, error_message\n"
        + "FROM collection_log\n"
        + "WHERE server_id = $1\n"
        + "AND   collection_time >= $2\n"
        + "AND   collection_time <= $3\n"
        + "AND   error_message IS NOT NULL\n"
        + "AND   (error_message LIKE '%" + ServerEpoch.IdentityChangesMeasurement + "=%'\n"
        + "   OR  error_message LIKE '%" + ServerEpoch.StatementsChangesMeasurement + "=%'\n"
        + "   OR  error_message LIKE '%" + ServerEpoch.PostmasterChangesMeasurement + "=%')\n"
        + "ORDER BY collection_time, collector_name";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The collectors one server runs: the four that observe an epoch (the two SQL Server carriers and the two PostgreSQL
    /// ones) and others, at the cadences the defaults give.</summary>
    private static string CollectorValues(int fillers)
    {
        var rows = new List<string>
        {
            $"(1, '{CpuUtilizationCollector.Instance.Name}', 1)",
            $"(2, '{WaitStatsCollector.Instance.Name}', 1)",
            $"(3, '{PgStatementStatsCollector.Instance.Name}', 1)",
            $"(4, '{PgWaitStatsCollector.Instance.Name}', 1)",
        };
        for (var i = 0; i < fillers; i++)
        {
            rows.Add($"({5 + i}, 'filler_{i:D2}', {(i % 3 == 0 ? 5 : 1)})");
        }

        return string.Join(", ", rows);
    }

    /// <summary>
    /// The rows: one per collector per interval per server from <see cref="Start"/> for <paramref name="days"/> days. Most notes are NULL
    /// and about one in thirty-seven is a short note. <c>pg_statement_stats</c> also carries the deallocation count its own label
    /// writes, and <c>wait_stats</c> a note that names a label without the equals sign. Then <see cref="MarkersPerWriter"/> marker rows
    /// per writer per server, thirty seconds after a day boundary, the statements and postmaster writers with a host note in front.
    /// </summary>
    internal static async Task SeedAsync(NpgsqlConnection connection, int servers, int days, int fillers, CancellationToken ct)
    {
        await using (var noise = new NpgsqlCommand($@"
INSERT INTO collect.collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
SELECT (s * 100000000000::bigint) + (c.idx * 100000000::bigint) + m, s, 'srv' || s, c.name,
       @start + (m * c.period) * interval '1 minute', 50, 'SUCCESS',
       CASE WHEN c.name = '{PgStatementStatsCollector.Instance.Name}' AND m % 11 = 0 THEN '{ServerEpoch.StatementsDeallocMeasurement}=3'
            WHEN c.name = '{WaitStatsCollector.Instance.Name}' AND m % 13 = 0 THEN 'the {ServerEpoch.IdentityChangesMeasurement} marker is prose here'
            WHEN m % 37 = 0 THEN 'short note ' || m
       END,
       10
FROM generate_series(1, @servers) AS s,
     (VALUES {CollectorValues(fillers)}) AS c(idx, name, period),
     LATERAL generate_series(0, (@days * 1440 / c.period) - 1) AS m", connection) { CommandTimeout = 600 })
        {
            noise.Parameters.Add(new NpgsqlParameter("start", NpgsqlDbType.Timestamp) { Value = Start });
            noise.Parameters.Add(new NpgsqlParameter("servers", NpgsqlDbType.Integer) { Value = servers });
            noise.Parameters.Add(new NpgsqlParameter("days", NpgsqlDbType.Integer) { Value = days });
            await noise.ExecuteNonQueryAsync(ct);
        }

        await using var markers = new NpgsqlCommand($@"
INSERT INTO collect.collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
SELECT 90000000000000::bigint + (s * 1000) + (w.idx * 10) + k, s, 'srv' || s, w.name,
       @start + (k * interval '1 day') + interval '30 seconds', 50, 'SUCCESS', w.note, 10
FROM generate_series(1, @servers) AS s,
     (VALUES (1, '{CpuUtilizationCollector.Instance.Name}', '{ServerEpoch.IdentityChangesMeasurement}=1'),
             (2, '{WaitStatsCollector.Instance.Name}', '{ServerEpoch.IdentityChangesMeasurement}=1'),
             (3, '{PgStatementStatsCollector.Instance.Name}', 'wall-clock budget (120s) reached; {ServerEpoch.StatementsChangesMeasurement}=1'),
             (4, '{PgWaitStatsCollector.Instance.Name}', 'cycle abandoned; {ServerEpoch.PostmasterChangesMeasurement}=1')) AS w(idx, name, note),
     generate_series(1, {MarkersPerWriter}) AS k", connection) { CommandTimeout = 600 };
        markers.Parameters.Add(new NpgsqlParameter("start", NpgsqlDbType.Timestamp) { Value = Start });
        markers.Parameters.Add(new NpgsqlParameter("servers", NpgsqlDbType.Integer) { Value = servers });
        await markers.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Applies the store's compression settings, compresses every chunk that ends before the last day as the store's policy does, and refreshes the statistics.</summary>
    internal static async Task CompressOlderChunksAsync(NpgsqlConnection connection, DateTime lastDayStart, CancellationToken ct)
    {
        /* The store's own runtime step: the hypertable's compression settings as the service leaves them (segmented by server and collector, #4951). */
        await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct);
        await using (var compress = new NpgsqlCommand(
            "SELECT count(compress_chunk(c, if_not_compressed => TRUE)) FROM show_chunks('collect.collection_log', older_than => @cutoff) AS c", connection)
        { CommandTimeout = 600 })
        {
            compress.Parameters.Add(new NpgsqlParameter("cutoff", NpgsqlDbType.Timestamp) { Value = lastDayStart });
            await compress.ExecuteNonQueryAsync(ct);
        }

        await ExecAsync(connection, "ANALYZE collect.collection_log", ct);
    }

    private static async Task<List<(DateTime At, string Collector, string? Note)>> ReadAsync(
        NpgsqlConnection connection, string sql, int serverId, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = from });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = to });
        var rows = new List<(DateTime, string, string?)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetDateTime(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
        }

        return rows;
    }

    [Fact]
    public async Task TheMarkerRead_ReturnsWhatTheReadBeforeItReturned_ForEveryWriter_InCompressedAndPlainChunks()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, Servers, Days, fillers: 6, ct);
            await CompressOlderChunksAsync(connection, Start.AddDays(Days - 1), ct);

            var end = Start.AddDays(Days);
            var perServer = Writers * MarkersPerWriter;

            /* The whole span, every server: all of the markers, from all four writers. */
            for (var server = 1; server <= Servers; server++)
            {
                var reference = await ReadAsync(connection, ReferenceSql, server, Start, end, ct);
                var read = await ReadAsync(connection, BaselineDiscontinuities.MarkerRowsSql, server, Start, end, ct);
                Assert.Equal(perServer, reference.Count);
                Assert.Equal(reference, read);
                Assert.Equal(4, read.Select(r => r.Collector).Distinct().Count());
            }

            /* The last day alone, where the chunk is plain: one marker per writer. */
            var lastDay = await ReadAsync(connection, BaselineDiscontinuities.MarkerRowsSql, 2, Start.AddDays(Days - 1), end, ct);
            Assert.Equal(await ReadAsync(connection, ReferenceSql, 2, Start.AddDays(Days - 1), end, ct), lastDay);
            Assert.Equal(Writers, lastDay.Count);

            /* A window that starts exactly on a marker's instant keeps it: both sides are inclusive. */
            var onTheEdge = await ReadAsync(connection, BaselineDiscontinuities.MarkerRowsSql, 1, Start.AddDays(1).AddSeconds(30), Start.AddDays(1).AddSeconds(30), ct);
            Assert.Equal(await ReadAsync(connection, ReferenceSql, 1, Start.AddDays(1).AddSeconds(30), Start.AddDays(1).AddSeconds(30), ct), onTheEdge);
            Assert.Equal(Writers, onTheEdge.Count);

            /* Windows with no marker in them, in a compressed chunk and in the plain one, and a server that has no rows. */
            foreach (var (server, from, to) in new[]
            {
                (2, Start.AddDays(1).AddHours(1), Start.AddDays(1).AddHours(5)),
                (3, Start.AddDays(Days - 1).AddHours(1), Start.AddDays(Days - 1).AddHours(5)),
                (99, Start, end),
            })
            {
                var read = await ReadAsync(connection, BaselineDiscontinuities.MarkerRowsSql, server, from, to, ct);
                Assert.Empty(read);
                Assert.Empty(await ReadAsync(connection, ReferenceSql, server, from, to, ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
