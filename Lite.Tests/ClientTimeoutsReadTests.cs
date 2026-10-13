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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The <c>get_client_timeouts</c> read (#5640) on a real DuckDB file: the range read with its ceiling, then the shared
/// <see cref="ClientTimeoutsRead"/> judgement on top of it. A storm (data, exact count and rate, samples are not the total), a quiet
/// window (the collector ran, the counter did not move: empty), a stale collector (unavailable), and an as_of ceiling that leaves later
/// rows out. The Darling twin runs the same shared text over PostgreSQL.
/// </summary>
public sealed class ClientTimeoutsReadTests : IClassFixture<SharedDuckDbFixture>
{
    private const int StormServer = 56401;
    private const int QuietServer = 56402;
    private const int StaleServer = 56403;

    private readonly DuckDbInitializer _duckDb;

    public ClientTimeoutsReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task AStorm_IsDataWithTheExactCountAndRate_AndSamplesAreSaidNotToBeTheTotal()
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 6; i++)
        {
            await LogAsync(100 + i, StormServer, now.AddSeconds(-30).AddMinutes(-i));
        }

        /* Five runs' counters moved by 30 each (the sixth run is the base): 150 timeouts over 5 minutes, 30 per minute. */
        for (var i = 0; i < 5; i++)
        {
            await RowAsync(200 + i, StormServer, now.AddSeconds(-28).AddMinutes(-i), "count", 30, null, null, null, null);
        }

        await RowAsync(300, StormServer, now.AddSeconds(-28), "sample", 0, "sales", "etl-job", "fp-a", "SELECT 1");
        await RowAsync(301, StormServer, now.AddSeconds(-28), "sample", 0, "sales", "etl-job", "fp-a", "SELECT 1");
        await RowAsync(302, StormServer, now.AddMinutes(-1).AddSeconds(-28), "sample", 0, "hr", "web", "fp-b", "SELECT 2");

        var (window, samples) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(StormServer, now.AddHours(-1), now, 20);
        var (kind, reading) = ClientTimeoutsRead.Classify(window, now);

        Assert.Equal(ClientTimeoutsKind.Data, kind);
        Assert.Equal(150, reading!.TotalCount);
        Assert.Equal(3, samples.Count);

        var json = JsonSerializer.Serialize(ClientTimeoutsRead.Payload(reading, window, samples, 20, 2000));
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(150, doc.RootElement.GetProperty("count").GetInt64());
        Assert.Equal(30d, doc.RootElement.GetProperty("rate_per_minute").GetDouble(), 1);
        Assert.Equal(3, doc.RootElement.GetProperty("sampled_events_in_range").GetInt64());
        Assert.Contains("never how many there were", doc.RootElement.GetProperty("samples_note").GetString());
        Assert.Equal("fp-a", doc.RootElement.GetProperty("top_statements")[0].GetProperty("statement_fingerprint").GetString());
        Assert.Equal("etl-job", doc.RootElement.GetProperty("top_client_apps")[0].GetProperty("client_app_name").GetString());
    }

    [Fact]
    public async Task TheSampleLimit_TruncatesAtTheBoundary_AndTheReturnedCountNeverExceedsIt()
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 4; i++)
        {
            await LogAsync(500 + i, StormServer, now.AddSeconds(-30).AddMinutes(-i));
            await RowAsync(510 + i, StormServer, now.AddSeconds(-28).AddMinutes(-i), "count", 2, null, null, null, null);
        }

        for (var i = 0; i < 3; i++)
        {
            await RowAsync(520 + i, StormServer, now.AddSeconds(-28).AddMinutes(-i), "sample", 0, "sales", "etl-job", "fp-a", "SELECT 1");
        }

        foreach (var (limit, truncated, returned) in new[] { (3, false, 3), (2, true, 2), (1, true, 1) })
        {
            var (window, samples) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(StormServer, now.AddHours(-1), now, limit);
            var (_, reading) = ClientTimeoutsRead.Classify(window, now);
            using var doc = JsonDocument.Parse(JsonSerializer.Serialize(ClientTimeoutsRead.Payload(reading!, window, samples, limit, 2000)));
            Assert.Equal(truncated, doc.RootElement.GetProperty("sampled_events_truncated").GetBoolean());
            Assert.Equal(returned, doc.RootElement.GetProperty("sampled_events_returned").GetInt32());
            Assert.Equal(returned, doc.RootElement.GetProperty("sampled_events").GetArrayLength());
            Assert.Equal(6, doc.RootElement.GetProperty("count").GetInt64());
        }
    }

    [Fact]
    public async Task AnAsOfJustAfterARun_StillCountsThatRunsRows_LikeTheAlertDoes()
    {
        /* A run is stamped when it starts; its rows carry the time the runner took a moment later. A range ending between the two
           must count the row, the allowance ReadSpan itself makes. */
        var newest = DateTime.UtcNow.AddMinutes(-5);
        newest = new DateTime(newest.Ticks - (newest.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        for (var i = 0; i < 5; i++)
        {
            await LogAsync(600 + i, StormServer, newest.AddMinutes(-i));
            await RowAsync(610 + i, StormServer, newest.AddMinutes(-i).AddSeconds(3), "count", 4, null, null, null, null);
        }

        var asOf = newest.AddSeconds(1);
        var (window, _) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(StormServer, asOf.AddHours(-1), asOf, 20);
        var (kind, reading) = ClientTimeoutsRead.Classify(window, asOf);

        Assert.Equal(ClientTimeoutsKind.Data, kind);
        Assert.Equal(16, reading!.TotalCount);
    }

    [Fact]
    public async Task AQuietWindow_WhoseCollectorRan_IsEmptyNotUnavailable()
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 5; i++)
        {
            await LogAsync(400 + i, QuietServer, now.AddSeconds(-30).AddMinutes(-i));
        }

        var (window, samples) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(QuietServer, now.AddHours(-1), now, 20);
        var (kind, reading) = ClientTimeoutsRead.Classify(window, now);

        Assert.Equal(ClientTimeoutsKind.Quiet, kind);
        Assert.Equal(0, reading!.TotalCount);
        Assert.Empty(samples);
        Assert.Contains("did not move", ClientTimeoutsRead.EmptyMessage("sql01", reading, 1));
    }

    [Fact]
    public async Task AStaleCollector_IsNotObserved_SoNoCountIsClaimed()
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 5; i++)
        {
            await LogAsync(500 + i, StaleServer, now.AddMinutes(-45).AddMinutes(-i));
        }

        await RowAsync(600, StaleServer, now.AddMinutes(-44), "count", 999, null, null, null, null);

        var (window, _) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(StaleServer, now.AddHours(-2), now, 20);
        var (kind, reading) = ClientTimeoutsRead.Classify(window, now);

        Assert.Equal(ClientTimeoutsKind.NotObserved, kind);
        Assert.Null(reading);
        Assert.Contains("not observed", ClientTimeoutsRead.UnavailableMessage("sql01", window, now, 2).Replace("has not observed", "not observed"));
    }

    [Fact]
    public async Task TheCeiling_LeavesRowsAfterAsOfOut()
    {
        var now = DateTime.UtcNow;
        for (var i = 0; i < 8; i++)
        {
            await LogAsync(700 + i, StormServer + 10, now.AddSeconds(-30).AddMinutes(-i));
        }

        await RowAsync(800, StormServer + 10, now.AddSeconds(-28), "count", 500, null, null, null, null);
        await RowAsync(801, StormServer + 10, now.AddMinutes(-5).AddSeconds(-28), "count", 7, null, null, null, null);

        var asOf = now.AddMinutes(-3);
        var (window, _) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(StormServer + 10, asOf.AddHours(-1), asOf, 20);
        var (_, reading) = ClientTimeoutsRead.Classify(window, asOf);

        Assert.Equal(7, reading!.TotalCount);
    }

    private async Task LogAsync(long logId, int serverId, DateTime time)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
VALUES ($1, $2, 'test-server', $3, $4, 5, 'SUCCESS')";
        command.Parameters.Add(new DuckDBParameter { Value = logId });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = AttentionWindowSql.CollectorName });
        command.Parameters.Add(new DuckDBParameter { Value = time });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task RowAsync(
        long id, int serverId, DateTime time, string kind, long count, string? db, string? app, string? fingerprint, string? text)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO attention_events
    (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_fingerprint, statement_text)
VALUES ($1, $2, $3, 'test-server', $4, $5, $6, $7, $8, $9)";
        command.Parameters.Add(new DuckDBParameter { Value = id });
        command.Parameters.Add(new DuckDBParameter { Value = time });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = kind });
        command.Parameters.Add(new DuckDBParameter { Value = count });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)app ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)fingerprint ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)text ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
