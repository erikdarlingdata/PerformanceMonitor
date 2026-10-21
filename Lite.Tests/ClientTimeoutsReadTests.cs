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
using PerformanceMonitorLite.Mcp;
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
    private const int BaselineServer = 56404;

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

    /// <summary>
    /// The precondition arm get_client_timeouts reaches when the window is not observed (#5640): the SAME call the tool makes, with the attention
    /// collector's catalog name. A missing capture session on the collector's latest run replaces the generic "has not observed" sentence, and a
    /// collector with no log row at all (disabled, never run) falls through to null so the tool's own unavailable sentence stands.
    /// </summary>
    [Fact]
    public async Task AMissingSession_OnTheLatestRun_IsAPreconditionSentence_AndNoLogRowFallsThrough()
    {
        var now = DateTime.UtcNow;
        var dataService = new LocalDataService(_duckDb);

        /* Never ran at all (a disabled collector writes nothing): the tool's own unavailable sentence stands. */
        Assert.Null(await McpRuntimePrecondition.StatusAsync(dataService, StormServer, "sql01", AttentionWindowSql.CollectorName));

        for (var i = 0; i < 5; i++)
        {
            await LogAsync(700 + i, StaleServer, now.AddMinutes(-45).AddMinutes(-i));
        }

        /* Ran, and the latest run succeeded: still nothing to say about a setup step. */
        Assert.Null(await McpRuntimePrecondition.StatusAsync(dataService, StaleServer, "sql01", AttentionWindowSql.CollectorName));

        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message)
VALUES (799, $1, 'test-server', $2, $3, 5, $4, 'The specified object PerformanceMonitor_Attention could not be found.')";
            command.Parameters.Add(new DuckDBParameter { Value = StaleServer });
            command.Parameters.Add(new DuckDBParameter { Value = AttentionWindowSql.CollectorName });
            command.Parameters.Add(new DuckDBParameter { Value = now.AddMinutes(-1) });
            command.Parameters.Add(new DuckDBParameter { Value = "SESSION_MISSING" });
            await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var json = await McpRuntimePrecondition.StatusAsync(dataService, StaleServer, "sql01", AttentionWindowSql.CollectorName);

        Assert.NotNull(json);
        Assert.Contains("\"precondition\"", json, StringComparison.Ordinal);
        Assert.Contains("attention collector", json, StringComparison.Ordinal);
        Assert.Contains("ALTER ANY EVENT SESSION", json, StringComparison.Ordinal);
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

    [Fact]
    public async Task AnAsOf_DoesNotLetTheRowAllowanceLeakIntoTheTopListsOrTheSampledEvents()
    {
        /* The as_of row allowance (windowEnd + 30 s) and the lower bound (windowStart) are wider than the span ReadSpan counts, which runs
           from the midpoint after the BASE run to the newest run + 30 s. The top lists and the samples are read over exactly the span, so
           the base run's samples and the rows between the newest run's skew and the allowance stay out (#5640, round 2). */
        var server = StormServer + 20;
        var newest = DateTime.UtcNow.AddMinutes(-5);
        newest = new DateTime(newest.Ticks - (newest.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        for (var i = 0; i < 5; i++)
        {
            await LogAsync(1000 + i, server, newest.AddMinutes(-i));
            await RowAsync(1010 + i, server, newest.AddMinutes(-i).AddSeconds(3), "count", 4, null, null, null, null);
        }

        /* The oldest run (newest - 4 min) is the base: its sample sits before the span. */
        await RowAsync(1020, server, newest.AddMinutes(-4).AddSeconds(3), "sample", 0, "basedb", "BaseApp", "fp-base", "SELECT base");
        await RowAsync(1021, server, newest.AddMinutes(-3).AddSeconds(3), "sample", 0, "spandb", "SpanApp", "fp-span", "SELECT span");
        /* Past the newest run + 30 s, inside the as_of allowance (as_of + 30 s). */
        await RowAsync(1022, server, newest.AddSeconds(30.5), "sample", 0, "latedb", "LateApp", "fp-late", "SELECT late");

        var asOf = newest.AddSeconds(1);
        var (window, samples) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(server, asOf.AddHours(-1), asOf, 20);
        var (kind, reading) = ClientTimeoutsRead.Classify(window, asOf);

        Assert.Equal(ClientTimeoutsKind.Data, kind);
        Assert.Equal(1, reading!.SampleTotal);
        Assert.Equal(new[] { "SpanApp" }, window.TopApplications.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "spandb" }, window.TopDatabases.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "fp-span" }, window.TopStatements.Select(t => t.Name).ToArray());
        Assert.Equal(new[] { "SELECT span" }, samples.Select(x => x.StatementText).ToArray());

        /* The invariant the answer states: no top list sums past the in-range sample count. */
        Assert.True(window.TopApplications.Sum(t => t.Samples) <= reading.SampleTotal);
        Assert.True(samples.Count <= reading.SampleTotal);
    }

    [Fact]
    public async Task Ties_AreOrderedTheSameWayInBothStores_BinaryNamesAndNewestIdFirst()
    {
        /* Count ties break by name in binary order (PostgreSQL's store collation and DuckDB's binary default must not pick different fifth
           entries: "App_b" < "app_a" in binary, the reverse under most locales), the sampled events break ties on the unique id, and the
           sample text of a fingerprint is the highest id's among same-instant samples (#5640, round 2). The Darling twin runs the same
           shared text over PostgreSQL. */
        var server = StormServer + 21;
        var newest = DateTime.UtcNow.AddMinutes(-1);
        newest = new DateTime(newest.Ticks - (newest.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        for (var i = 0; i < 3; i++)
        {
            await LogAsync(1100 + i, server, newest.AddMinutes(-i));
            await RowAsync(1110 + i, server, newest.AddMinutes(-i).AddSeconds(3), "count", 5, null, null, null, null);
        }

        var at = newest.AddSeconds(3);
        long id = 1200;
        foreach (var app in new[] { "a1", "a2", "a3", "a4" })
        {
            await RowAsync(id, server, at, "sample", 0, "db1", app, null, "t" + id);
            id++;
            await RowAsync(id, server, at, "sample", 0, "db1", app, null, "t" + id);
            id++;
        }

        await RowAsync(id, server, at, "sample", 0, "db1", "app_a", null, "t" + id);
        id++;
        await RowAsync(id, server, at, "sample", 0, "db1", "App_b", null, "t" + id);
        id++;
        var firstFingerprintId = id;
        foreach (var n in new[] { 1, 2, 3 })
        {
            await RowAsync(id, server, at, "sample", 0, null, null, "fp-pick", "pick" + n);
            id++;
        }

        var (window, samples) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(server, newest.AddHours(-1), newest, 20);

        Assert.Equal(new[] { "a1", "a2", "a3", "a4", "App_b" }, window.TopApplications.Select(t => t.Name).ToArray());
        Assert.Equal("pick3", window.TopStatements.Single().SampleText);

        var expected = Enumerable.Range(1200, (int)(id - 1200)).Reverse()
            .Select(n => n >= firstFingerprintId ? "pick" + (n - firstFingerprintId + 1) : "t" + n).ToArray();
        Assert.Equal(expected, samples.Select(x => x.StatementText).ToArray());

        var (_, page) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(server, newest.AddHours(-1), newest, 4);
        Assert.Equal(expected.Take(5).ToArray(), page.Select(x => x.StatementText).ToArray());
    }

    /// <summary>One successful run a minute for <paramref name="minutes"/> minutes ending the minute before <paramref name="now"/>, and a count row
    /// two seconds into each run's minute holding <paramref name="perMinute"/> attentions. Bulk inserts, so a multi-day server is one statement each.</summary>
    private async Task SeedSteadyAsync(int serverId, DateTime now, int minutes, int perMinute, long idBase)
    {
        var first = now.AddMinutes(-minutes);
        var last = now.AddMinutes(-1);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var log = connection.CreateCommand();
        log.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
SELECT $1 + row_number() OVER (ORDER BY t), $2, 'test-server', $3, t, 5, 'SUCCESS'
FROM generate_series($4::TIMESTAMP, $5::TIMESTAMP, INTERVAL 1 MINUTE) AS g(t)";
        log.Parameters.Add(new DuckDBParameter { Value = idBase });
        log.Parameters.Add(new DuckDBParameter { Value = serverId });
        log.Parameters.Add(new DuckDBParameter { Value = AttentionWindowSql.CollectorName });
        log.Parameters.Add(new DuckDBParameter { Value = first });
        log.Parameters.Add(new DuckDBParameter { Value = last });
        await log.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);

        using var counts = connection.CreateCommand();
        counts.CommandText = @"
INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count)
SELECT $1 + row_number() OVER (ORDER BY t), t + INTERVAL 2 SECOND, $2, 'test-server', $3, $4
FROM generate_series($5::TIMESTAMP, $6::TIMESTAMP, INTERVAL 1 MINUTE) AS g(t)";
        counts.Parameters.Add(new DuckDBParameter { Value = idBase + 1_000_000 });
        counts.Parameters.Add(new DuckDBParameter { Value = serverId });
        counts.Parameters.Add(new DuckDBParameter { Value = AttentionWindowSql.KindCount });
        counts.Parameters.Add(new DuckDBParameter { Value = (long)perMinute });
        counts.Parameters.Add(new DuckDBParameter { Value = first });
        counts.Parameters.Add(new DuckDBParameter { Value = last });
        await counts.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task TheBaseline_OfASteadyServer_IsReadFromDuckDb_AndIsItsNormalRate()
    {
        /* #5736: the real DuckDB read of the shared per-minute statement, which the engine tests only fake. Two days of one run a minute with a
           count row of 150 in each: every 3-minute average is 150, so the p99.9 is 150. */
        var now = DateTime.UtcNow;
        now = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerMinute), DateTimeKind.Utc);
        await SeedSteadyAsync(BaselineServer, now, 2 * 1440, 150, 9_000_000);

        var baseline = await new LocalDataService(_duckDb).GetAttentionBaselineAsync(BaselineServer, now);

        Assert.NotNull(baseline);
        Assert.Equal(150d, baseline!.P999ThreeMinuteAverage, 3);
        Assert.Equal(2 * 1440, baseline.MinutesObserved);
        Assert.Null(await new LocalDataService(_duckDb).GetAttentionBaselineAsync(BaselineServer + 1, now));
    }

    [Fact]
    public async Task TheBaseline_OfAServerWithUnderADayOfCapture_IsNull_AndSoIsAStrayRunPlusAnHour()
    {
        var now = DateTime.UtcNow;
        now = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerMinute), DateTimeKind.Utc);

        /* 12 hours is under the day. */
        await SeedSteadyAsync(BaselineServer, now, 720, 150, 9_000_000);
        Assert.Null(await new LocalDataService(_duckDb).GetAttentionBaselineAsync(BaselineServer, now));

        /* One stray run two days ago plus an hour of recent capture spans two days but observed 61 minutes. */
        var stray = BaselineServer + 2;
        await LogAsync(9_500_000, stray, now.AddDays(-2));
        await SeedSteadyAsync(stray, now, 60, 150, 9_600_000);
        Assert.Null(await new LocalDataService(_duckDb).GetAttentionBaselineAsync(stray, now));
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
