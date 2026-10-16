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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// <c>Lite/tools/attention-rate-measure.sql</c> (#5736) on a real DuckDB file, with planted attention rows: the query pm-worker runs
/// on the field stores to measure the per-minute attention rate before the Client Timeouts bars are set from it. The shipped file is
/// read from the repo and run after the two session variables it reads are set, as the DuckDB CLI runs it. The Darling twin runs the
/// same rule over PostgreSQL (<c>AttentionRateMeasureScriptLiveTests</c>); the planted data and the expected numbers are the same.
/// </summary>
public sealed class AttentionRateMeasureScriptTests : IClassFixture<SharedDuckDbFixture>
{
    private const int QuietOneSpikeServer = 57361;
    private const int SpikyServer = 57362;
    private const int SilentServer = 57363;
    private const int HoleServer = 57364;
    private static readonly DateTime Day = new(2001, 3, 1, 0, 0, 0, DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;

    public AttentionRateMeasureScriptTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task AServerWithOneAttentionInAHundredRunMinutes_HasAMedianOfZero_BecauseARunThatCountedNothingIsAZeroBucket()
    {
        var rows = await PlantAndRunAsync(PlantStandardAsync);
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
    }

    [Fact]
    public async Task ACollectionHole_IsNotQuietMinutes_ItShowsInTheCoverageColumnsAndEveryPercentileIsOverTheRunMinutes()
    {
        var rows = await PlantAndRunAsync(PlantHoleAsync);
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
    }

    [Fact]
    public async Task APlantedSpike_LandsInTheP999_TheMaxAndTheTopMinutes_AndCountRowsInOneMinuteAreSummed()
    {
        var rows = await PlantAndRunAsync(PlantStandardAsync);
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
    }

    [Fact]
    public async Task TheFleetRow_CarriesTheMinMedianAndMaxOfThePerServerP999_AndANeverCountedServerIsInIt()
    {
        var rows = await PlantAndRunAsync(PlantStandardAsync);

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
    }

    private static ResultRow ServerRow(List<ResultRow> rows, int serverId) =>
        rows.Single(r => r.Text("scope") == "server" && r.Long("server_id") == serverId);

    private async Task<List<ResultRow>> PlantAndRunAsync(Func<DuckDBConnection, System.Threading.CancellationToken, Task> plant)
    {
        var ct = TestContext.Current.CancellationToken;
        var script = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "tools", "attention-rate-measure.sql"));

        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(ct);

        await plant(connection, ct);

        await ExecAsync(connection, "SET VARIABLE range_from = '2001-03-01 00:00:00'", ct);
        await ExecAsync(connection, "SET VARIABLE range_to = '2001-03-02 00:00:00'", ct);

        var rows = new List<ResultRow>();
        using var command = connection.CreateCommand();
        command.CommandText = script;
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new Dictionary<string, object?>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(new ResultRow(values));
        }

        return rows;
    }

    private static async Task PlantStandardAsync(DuckDBConnection connection, System.Threading.CancellationToken ct)
    {
        long id = 1;

        /* 100 one-minute runs (minutes 0 to 99), one attention in the middle one. */
        id = await RunsAsync(connection, id, QuietOneSpikeServer, 0, 99, ct);
        await ExecAsync(connection, RowSql(id++, QuietOneSpikeServer, Day.AddMinutes(50).AddSeconds(20), "count", 10), ct);

        /* 1000 one-minute runs (minutes 0 to 999) with three spikes. */
        id = await RunsAsync(connection, id, SpikyServer, 0, 999, ct);
        await ExecAsync(connection, RowSql(id++, SpikyServer, Day.AddMinutes(250).AddSeconds(5), "count", 40), ct);
        await ExecAsync(connection, RowSql(id++, SpikyServer, Day.AddMinutes(500).AddSeconds(5), "count", 80), ct);
        await ExecAsync(connection, RowSql(id++, SpikyServer, Day.AddMinutes(720).AddSeconds(5), "count", 60), ct);
        await ExecAsync(connection, RowSql(id++, SpikyServer, Day.AddMinutes(720).AddSeconds(50), "count", 40), ct);
        await ExecAsync(connection, RowSql(id++, SpikyServer, Day.AddMinutes(720).AddSeconds(5), "sample", 0), ct);

        /* A collector that ran for ten minutes and never counted anything. */
        id = await RunsAsync(connection, id, SilentServer, 0, 9, ct);

        /* A run that did not read the counter is not proof of observation: its minute is not a bucket. */
        await ExecAsync(connection, LogSql(id++, SilentServer, Day.AddMinutes(30), "SESSION_MISSING"), ct);
    }

    private static async Task PlantHoleAsync(DuckDBConnection connection, System.Threading.CancellationToken ct)
    {
        /* Runs in minutes 1 to 30 and 71 to 100, one attention per run; minutes 31 to 70 have no run at all. */
        long id = 1;
        foreach (var (first, last) in new[] { (1, 30), (71, 100) })
        {
            for (var minute = first; minute <= last; minute++)
            {
                await ExecAsync(connection, LogSql(id++, HoleServer, Day.AddMinutes(minute).AddSeconds(2)), ct);
                await ExecAsync(connection, RowSql(id++, HoleServer, Day.AddMinutes(minute).AddSeconds(3), "count", 1), ct);
            }
        }
    }

    private static async Task<long> RunsAsync(
        DuckDBConnection connection, long id, int serverId, int firstMinute, int lastMinute, System.Threading.CancellationToken ct)
    {
        for (var minute = firstMinute; minute <= lastMinute; minute++)
        {
            await ExecAsync(connection, LogSql(id++, serverId, Day.AddMinutes(minute).AddSeconds(2)), ct);
        }

        return id;
    }

    private static string Stamp(DateTime time) => time.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    private static string LogSql(long logId, int serverId, DateTime time, string status = "SUCCESS") =>
        $"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status) VALUES ({logId}, {serverId}, 'rate-measure', 'attention', TIMESTAMP '{Stamp(time)}', 5, '{status}')";

    private static string RowSql(long id, int serverId, DateTime time, string kind, long count) =>
        $"INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count) VALUES ({id}, TIMESTAMP '{Stamp(time)}', {serverId}, 'rate-measure', '{kind}', {count})";

    private static async Task ExecAsync(DuckDBConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.Collectors")))
            {
                return dir.FullName;
            }
        }

        throw new DirectoryNotFoundException($"could not locate the repo root walking up from {thisFile}");
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
