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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>Darling/tools/attention-rate-measure.sql</c> (#5736) on a real PostgreSQL store, with planted attention rows: the script pm-worker
/// runs on the field stores to measure the per-minute attention rate before the Client Timeouts bars are set from it. The script is
/// read from the repo and run with its two psql variables filled in by text (Npgsql does not take psql's <c>:'from'</c>). The planted
/// day is in 2001 so no other test's rows, and no real capture, fall inside the range. Gated on DARLING_TEST_PG.
/// </summary>
[Collection("live-postgres")]
public sealed class AttentionRateMeasureScriptLiveTests
{
    private const int QuietOneSpikeServer = -5736001;
    private const int SpikyServer = -5736002;
    private const int SilentServer = -5736003;
    private const int HoleServer = -5736004;
    private static readonly DateTime Day = new(2001, 3, 1, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task AServerWithOneAttentionInAHundredRunMinutes_HasAMedianOfZero_BecauseARunThatCountedNothingIsAZeroBucket()
    {
        await RunAsync(PlantStandardAsync, async rows =>
        {
            var server = ServerRow(rows, QuietOneSpikeServer);

            /* 100 one-minute runs, one with an attention: the 99 runs that counted nothing are zero buckets. */
            Assert.Equal(100L, server.Long("minutes_in_span"));
            Assert.Equal(100L, server.Long("minutes_with_a_run"));
            Assert.Equal(0L, server.Long("longest_gap_minutes"));
            Assert.Equal(10L, server.Long("attentions"));
            Assert.Equal(0m, server.Decimal("p50"));
            Assert.Equal(10L, server.Long("max_per_minute"));
            Assert.Equal(0.10m, server.Decimal("p99"));
            Assert.Equal("2001-03-01 00:50=10", server.Text("top_minutes"));
        });
    }

    [Fact]
    public async Task ACollectionHole_IsNotQuietMinutes_ItShowsInTheCoverageColumnsAndEveryPercentileIsOverTheRunMinutes()
    {
        await RunAsync(PlantHoleAsync, async rows =>
        {
            var server = ServerRow(rows, HoleServer);

            /* Runs in minutes 1 to 30 and 71 to 100, one attention per run. The span is 100 minutes but only 60 had a run; the
               40 minutes between are not observed, so they are not buckets. */
            Assert.Equal(100L, server.Long("minutes_in_span"));
            Assert.Equal(60L, server.Long("minutes_with_a_run"));
            Assert.Equal(40L, server.Long("longest_gap_minutes"));
            Assert.Equal(60L, server.Long("attentions"));
            Assert.Equal(1m, server.Decimal("p50"));
            Assert.Equal(1m, server.Decimal("p99"));
            Assert.Equal(1m, server.Decimal("p999"));
            Assert.Equal(1L, server.Long("max_per_minute"));

            /* No 3-minute window crosses the hole, and every window inside a run is a full three-minute window. */
            Assert.Equal(1m, server.Decimal("p999_3min_avg"));

            var fleet = rows.Single(r => r.Text("scope") == "fleet");
            Assert.Equal(100L, fleet.Long("minutes_in_span"));
            Assert.Equal(60L, fleet.Long("minutes_with_a_run"));
            Assert.Equal(40L, fleet.Long("longest_gap_minutes"));
        });
    }

    [Fact]
    public async Task APlantedSpike_LandsInTheP999_TheMaxAndTheTopMinutes_AndCountRowsInOneMinuteAreSummed()
    {
        await RunAsync(PlantStandardAsync, async rows =>
        {
            var server = ServerRow(rows, SpikyServer);

            /* 1000 minutes with 40, 80 and 100 in three minutes (the 100 as two count rows of 60 and 40 in one minute, plus a
               sample row that carries no count). The p99.9 sits between the second and third largest buckets. */
            Assert.Equal(1000L, server.Long("minutes_in_span"));
            Assert.Equal(1000L, server.Long("minutes_with_a_run"));
            Assert.Equal(220L, server.Long("attentions"));
            Assert.Equal(80.02m, server.Decimal("p999"));
            Assert.Equal(100L, server.Long("max_per_minute"));
            Assert.Equal(0m, server.Decimal("p50"));
            Assert.Equal("2001-03-01 12:00=100; 2001-03-01 08:20=80; 2001-03-01 04:10=40", server.Text("top_minutes"));

            /* The alert averages over three minutes, so an isolated one-minute spike of 100 reads as a third of that. */
            Assert.Equal(33.33m, server.Decimal("p999_3min_avg"));
        });
    }

    [Fact]
    public async Task TheFleetRow_CarriesTheMinMedianAndMaxOfThePerServerP999_AndANeverCountedServerIsInIt()
    {
        await RunAsync(PlantStandardAsync, async rows =>
        {
            Assert.Equal(new[] { "server", "server", "server", "fleet" }, rows.Select(r => r.Text("scope")).ToArray());

            var fleet = rows.Single(r => r.Text("scope") == "fleet");
            Assert.Equal("(3 servers)", fleet.Text("server_name"));
            Assert.Equal(1110L, fleet.Long("minutes_in_span"));
            Assert.Equal(1110L, fleet.Long("minutes_with_a_run"));
            Assert.Equal(0L, fleet.Long("longest_gap_minutes"));
            Assert.Equal(230L, fleet.Long("attentions"));
            Assert.Equal(100L, fleet.Long("max_per_minute"));

            /* Per-server p99.9: the silent server 0, the one-spike server 9.01, the spiky server 80.02. */
            Assert.Equal(0m, ServerRow(rows, SilentServer).Decimal("p999"));
            Assert.Equal(0m, fleet.Decimal("fleet_p999_min"));
            Assert.Equal(9.01m, fleet.Decimal("fleet_p999_median"));
            Assert.Equal(80.02m, fleet.Decimal("fleet_p999_max"));
        });
    }

    private static ResultRow ServerRow(List<ResultRow> rows, int serverId) =>
        rows.Single(r => r.Text("scope") == "server" && r.Long("server_id") == serverId);

    private static async Task RunAsync(
        Func<NpgsqlConnection, CancellationToken, Task> plant, Func<List<ResultRow>, Task> assertions)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live attention-rate measurement script test.");

        var ct = TestContext.Current.CancellationToken;

        /* The shipped script, as an operator runs it, with psql's two variables filled in by text. */
        var script = RepoFile.ReadRepoFile("Darling", "tools", "attention-rate-measure.sql")
            .Replace(":'from'", "'2001-03-01 00:00:00'", StringComparison.Ordinal)
            .Replace(":'to'", "'2001-03-02 00:00:00'", StringComparison.Ordinal);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await plant(connection, ct);

            var rows = new List<ResultRow>();
            using (var command = new NpgsqlCommand(script, connection))
            using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var values = new Dictionary<string, object?>();
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        values[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    }

                    rows.Add(new ResultRow(values));
                }
            }

            await assertions(rows);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PlantStandardAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        long id = 1;

        /* 100 one-minute runs (minutes 0 to 99), one attention in the middle one. */
        id = await RunsAsync(connection, id, QuietOneSpikeServer, 0, 99, ct);
        await RowAsync(connection, id++, QuietOneSpikeServer, Day.AddMinutes(50).AddSeconds(20), "count", 10, ct);

        /* 1000 one-minute runs (minutes 0 to 999) with three spikes. */
        id = await RunsAsync(connection, id, SpikyServer, 0, 999, ct);
        await RowAsync(connection, id++, SpikyServer, Day.AddMinutes(250).AddSeconds(5), "count", 40, ct);
        await RowAsync(connection, id++, SpikyServer, Day.AddMinutes(500).AddSeconds(5), "count", 80, ct);
        await RowAsync(connection, id++, SpikyServer, Day.AddMinutes(720).AddSeconds(5), "count", 60, ct);
        await RowAsync(connection, id++, SpikyServer, Day.AddMinutes(720).AddSeconds(50), "count", 40, ct);
        await RowAsync(connection, id++, SpikyServer, Day.AddMinutes(720).AddSeconds(5), "sample", 0, ct);

        /* A collector that ran for ten minutes and never counted anything. */
        id = await RunsAsync(connection, id, SilentServer, 0, 9, ct);

        /* A run that did not read the counter is not proof of observation: its minute is not a bucket. */
        await LogAsync(connection, id++, SilentServer, Day.AddMinutes(30), ct, status: "SESSION_MISSING");
    }

    private static async Task PlantHoleAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* Runs in minutes 1 to 30 and 71 to 100, one attention per run; minutes 31 to 70 have no run at all. */
        long id = 1;
        foreach (var (first, last) in new[] { (1, 30), (71, 100) })
        {
            for (var minute = first; minute <= last; minute++)
            {
                await LogAsync(connection, id++, HoleServer, Day.AddMinutes(minute).AddSeconds(2), ct);
                await RowAsync(connection, id++, HoleServer, Day.AddMinutes(minute).AddSeconds(3), "count", 1, ct);
            }
        }
    }

    private static async Task<long> RunsAsync(
        NpgsqlConnection connection, long id, int serverId, int firstMinute, int lastMinute, CancellationToken ct)
    {
        for (var minute = firstMinute; minute <= lastMinute; minute++)
        {
            await LogAsync(connection, id++, serverId, Day.AddMinutes(minute).AddSeconds(2), ct);
        }

        return id;
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "attention_events", "collection_log" })
        {
            using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ($1, $2, $3, $4)", connection);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, QuietOneSpikeServer);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, SpikyServer);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, SilentServer);
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, HoleServer);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task LogAsync(
        NpgsqlConnection connection, long logId, int serverId, DateTime time, CancellationToken ct, string status = "SUCCESS")
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) VALUES ($1, $2, 'rate-measure', 'attention', $3, 5, $4)",
            connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, logId);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Text, status);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RowAsync(
        NpgsqlConnection connection, long id, int serverId, DateTime time, string kind, long count, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            @"INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count)
              VALUES ($1, $2, $3, 'rate-measure', $4, $5)", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, id);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, kind);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, count);
        await command.ExecuteNonQueryAsync(ct);
    }

    private sealed class ResultRow
    {
        private readonly Dictionary<string, object?> _values;

        public ResultRow(Dictionary<string, object?> values) => _values = values;

        public string? Text(string column) => _values[column] as string;

        public long Long(string column) => Convert.ToInt64(_values[column], CultureInfo.InvariantCulture);

        public decimal Decimal(string column) => Convert.ToDecimal(_values[column], CultureInfo.InvariantCulture);
    }
}
