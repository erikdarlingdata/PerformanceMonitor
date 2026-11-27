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
/// #9507 on a real DuckDB file: an attention sample under the 1 s floor that matches a captured call (an <c>aborted</c> row on the same
/// session and database, inside the match window) is judged, and get_client_timeouts lists the call with how long it ran. The twin of
/// Darling's <c>AttentionWindowReadLiveTests</c> and <c>ClientTimeoutsToolLiveTests</c> cases. The match itself is the shared statement's;
/// these tests read what Lite's reads do with it. Edge times come from the shared window constants, never from literals.
/// </summary>
public sealed class ClientTimeoutsCapturedCallMatchTests : IClassFixture<SharedDuckDbFixture>
{
    private const int Lookback = 30;
    private const int WindowServer = 95071;
    private const int BaselineServer = 95072;
    private const int ToolServer = 95073;
    private const int EdgeServer = 95074;
    private const int OtherServer = 95075;
    private const int MetadataServer = 95076;
    private const int NoCallServer = 95077;
    private const int OverFloorServer = 95078;
    private const int ExcludedAppServer = 95079;
    private const int ShortCallServer = 95080;

    private static readonly TimeSpan MatchAfter = TimeSpan.FromMicroseconds(AttentionWindowSql.CapturedCallMatchAfterUs);
    private static readonly TimeSpan MatchBefore = TimeSpan.FromMicroseconds(AttentionWindowSql.CapturedCallMatchBeforeUs);

    private readonly DuckDbInitializer _duckDb;
    private long _id = 1;

    public ClientTimeoutsCapturedCallMatchTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task AnUnderFloorSample_ThatMatchesAnAbortedCall_IsMatchedNotShort_InTheSampleGroupsAndTheEvaluator()
    {
        var (at, eventTime) = await SeedJudgedSpanAsync(WindowServer);

        /* Four 4 ms attentions. Sessions 1 and 2 have the call they cancelled captured, sessions 3 and 4 do not (a call on session 9 is another session's). */
        for (var session = 1; session <= 4; session++)
        {
            await RowAsync(WindowServer, at, "sample", "db1", "app", null, null, durationUs: 4_000, eventTime: eventTime, sessionId: session);
        }

        await RowAsync(WindowServer, at, "aborted", "db1", "app", "fp-call", "dbo.A: EXEC dbo.A", durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 1);
        await RowAsync(WindowServer, at, "aborted", "db1", "app", "fp-call", "dbo.B: EXEC dbo.B", durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 2);
        await RowAsync(WindowServer, at, "aborted", "db1", "app", "fp-call", "dbo.C: EXEC dbo.C", durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 9);

        var window = await new LocalDataService(_duckDb).GetAttentionWindowAsync(WindowServer, Lookback);

        /* The call rows are not samples: four sampled events, two matched and two short. */
        Assert.Equal(4, window.SampleGroups.Sum(g => g.Samples));
        Assert.Equal(2, window.SampleGroups.Sum(g => g.MatchedSamples));
        Assert.Equal(2, window.SampleGroups.Sum(g => g.ShortSamples));

        var reading = AttentionEvaluator.Read(window, DateTime.UtcNow)!;
        Assert.Equal(2, reading.MatchedSamples);
        Assert.Equal(2, reading.ShortSamples);

        /* Only the two short samples no call explains are left out: the exact count x 2/4. */
        var finding = AttentionEvaluator.Evaluate(reading, 1, 10_000);
        Assert.NotNull(finding);
        Assert.True(reading.TotalCount > 0);
        Assert.Equal(reading.TotalCount * 2 / 4.0, finding!.JudgedCount, 6);
        Assert.Equal(2, finding.MatchedCalls);
    }

    [Fact]
    public async Task TheBaseline_UsesTheSameMatchRule_AMatchedUnderFloorSampleIsKept_ExceptForAToolsMetadataCall()
    {
        var t0 = DateTime.UtcNow.Date.AddDays(-2).AddHours(12);
        for (var m = 0; m < 3; m++)
        {
            await LogAsync(BaselineServer, t0.AddMinutes(m));
        }

        /* Minute 0: four 4 ms samples, two with a call. The unmatched two are left out: 100 x 2/4 = 50. */
        var at0 = t0.AddSeconds(2);
        await RowAsync(BaselineServer, at0, "count", null, null, null, null, count: 100);
        for (var session = 1; session <= 4; session++)
        {
            await RowAsync(BaselineServer, at0, "sample", "db1", "app", null, null, durationUs: 4_000, eventTime: at0.AddSeconds(-1), sessionId: session);
        }

        await RowAsync(BaselineServer, at0, "aborted", "db1", "app", "fp", "dbo.A: EXEC dbo.A", durationUs: 30_000_000, eventTime: at0.AddSeconds(-1).AddMilliseconds(50), sessionId: 1);
        await RowAsync(BaselineServer, at0, "aborted", "db1", "app", "fp", "dbo.B: EXEC dbo.B", durationUs: 30_000_000, eventTime: at0.AddSeconds(-1).AddMilliseconds(50), sessionId: 2);

        /* Minute 1: an azdata attention whose matched call is metadata work is left out, an app attention with a call is kept: 60 x 1/2 = 30. */
        var at1 = t0.AddMinutes(1).AddSeconds(2);
        await RowAsync(BaselineServer, at1, "count", null, null, null, null, count: 60);
        await RowAsync(BaselineServer, at1, "sample", "master", "azdata", null, null, durationUs: 4_000, eventTime: at1.AddSeconds(-1), sessionId: 11);
        await RowAsync(BaselineServer, at1, "sample", "db1", "app", null, null, durationUs: 4_000, eventTime: at1.AddSeconds(-1), sessionId: 12);
        await RowAsync(BaselineServer, at1, "aborted", "master", "azdata", "fp", "create table #SVer(ID int)", durationUs: 30_000_000, eventTime: at1.AddSeconds(-1).AddMilliseconds(50), sessionId: 11);
        await RowAsync(BaselineServer, at1, "aborted", "db1", "app", "fp", "dbo.C: EXEC dbo.C", durationUs: 30_000_000, eventTime: at1.AddSeconds(-1).AddMilliseconds(50), sessionId: 12);

        /* Minute 2: no call rows at all, so every under-floor sample is left out as before: 40 x 0/2 = 0. */
        var at2 = t0.AddMinutes(2).AddSeconds(2);
        await RowAsync(BaselineServer, at2, "count", null, null, null, null, count: 40);
        await RowAsync(BaselineServer, at2, "sample", "db1", "app", null, null, durationUs: 4_000, eventTime: at2.AddSeconds(-1), sessionId: 21);
        await RowAsync(BaselineServer, at2, "sample", "db1", "app", null, null, durationUs: 4_000, eventTime: at2.AddSeconds(-1), sessionId: 22);

        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = AttentionWindowSql.BaselineMinutes("v_collection_log", "v_attention_events");
        command.Parameters.Add(new DuckDBParameter { Value = BaselineServer });
        command.Parameters.Add(new DuckDBParameter { Value = t0 });
        command.Parameters.Add(new DuckDBParameter { Value = t0.AddMinutes(3) });
        var got = new List<long>();
        using (var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken))
        {
            while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            {
                got.Add(reader.GetInt64(1));
            }
        }

        Assert.Equal(new long[] { 50, 30, 0 }, got.ToArray());
    }

    [Fact]
    public async Task AnUnderFloorAttention_ThatMatchesACapturedAbortedCall_IsJudged_AndEachCallIsListedWithHowLongItRan()
    {
        var (at, eventTime) = await SeedJudgedSpanAsync(ToolServer);

        /* Eight 4 ms attentions, each from its own session, each with the 30 s call it cancelled captured 20 ms later. */
        for (var i = 0; i < 8; i++)
        {
            await RowAsync(ToolServer, at, "sample", "db1", "etl", null, null, durationUs: 4_000, eventTime: eventTime, sessionId: 100 + i);
            await RowAsync(ToolServer, at, "aborted", "db1", "etl", "fp-call", "dbo.GetOrders: EXEC dbo.GetOrders @id = 1",
                durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(20), sessionId: 100 + i, host: "web01");
        }

        var (reading, body) = await ReadToolAsync(ToolServer);

        Assert.Equal(55, body.GetProperty("count").GetInt64());
        Assert.Equal(8, body.GetProperty("sampled_events_in_range").GetInt64());
        var excluded = body.GetProperty("excluded_from_alert");
        Assert.Equal(0, excluded.GetProperty("below_duration_floor_events").GetInt64());
        Assert.Equal(8, excluded.GetProperty("matched_calls").GetInt64());
        Assert.Equal(55.0, excluded.GetProperty("count_judged").GetDouble());

        var calls = body.GetProperty("matched_calls").EnumerateArray().ToList();
        Assert.Equal(8, calls.Count);
        Assert.All(calls, c =>
        {
            Assert.Equal(30.0, c.GetProperty("elapsed_seconds").GetDouble());
            Assert.Equal("db1", c.GetProperty("database_name").GetString());
            Assert.Equal("etl", c.GetProperty("client_app_name").GetString());
            Assert.Equal("web01", c.GetProperty("client_hostname").GetString());
            Assert.Contains("dbo.GetOrders", c.GetProperty("statement").GetString(), StringComparison.Ordinal);
        });
        Assert.Equal(Enumerable.Range(100, 8), calls.Select(c => c.GetProperty("session_id").GetInt32()).OrderBy(s => s));
        Assert.False(body.GetProperty("matched_calls_truncated").GetBoolean());

        var listed = body.GetProperty("sampled_events").EnumerateArray().ToList();
        Assert.Equal(8, listed.Count);
        Assert.All(listed, e =>
        {
            Assert.True(e.GetProperty("matched_call").GetBoolean());
            Assert.False(e.GetProperty("below_duration_floor").GetBoolean());
        });
        Assert.Equal(8, reading.MatchedSamples);
        Assert.Equal(0, reading.ShortSamples);
    }

    [Fact]
    public async Task ACallOnAnotherSession_AnotherDatabase_AnotherServer_OrOutsideTheMatchWindow_IsNotAMatch_ButTheWindowEdgesAre()
    {
        var (at, eventTime) = await SeedJudgedSpanAsync(EdgeServer);

        /* Seven 4 ms attentions, one session each. Only the last two have a call inside the window, at its two edges. */
        var cases = new (int Session, string Db, TimeSpan Offset, int CallSession, int CallServer)[]
        {
            (201, "db1", TimeSpan.FromMilliseconds(10), 299, EdgeServer),
            (202, "db1", MatchAfter + TimeSpan.FromMilliseconds(1), 202, EdgeServer),
            (203, "db2", TimeSpan.FromMilliseconds(10), 203, EdgeServer),
            (204, "db1", -(MatchBefore + TimeSpan.FromMilliseconds(1)), 204, EdgeServer),
            (207, "db1", TimeSpan.FromMilliseconds(10), 207, OtherServer),
            (205, "db1", MatchAfter, 205, EdgeServer),
            (206, "db1", -MatchBefore, 206, EdgeServer),
        };
        foreach (var (session, db, offset, callSession, callServer) in cases)
        {
            await RowAsync(EdgeServer, at, "sample", db, "etl", null, null, durationUs: 4_000, eventTime: eventTime, sessionId: session);
            await RowAsync(callServer, at, "aborted", "db1", "etl", "fp-call", "dbo.Slow: EXEC dbo.Slow",
                durationUs: 30_000_000, eventTime: eventTime + offset, sessionId: callSession);
        }

        var (_, body) = await ReadToolAsync(EdgeServer);

        Assert.Equal(7, body.GetProperty("sampled_events_in_range").GetInt64());
        var excluded = body.GetProperty("excluded_from_alert");
        Assert.Equal(2, excluded.GetProperty("matched_calls").GetInt64());
        Assert.Equal(5, excluded.GetProperty("below_duration_floor_events").GetInt64());
        Assert.Equal(55 * 2 / 7.0, excluded.GetProperty("count_judged").GetDouble(), 1);
        Assert.Equal(new[] { 205, 206 }, body.GetProperty("matched_calls").EnumerateArray().Select(c => c.GetProperty("session_id").GetInt32()).OrderBy(s => s).ToArray());
    }

    [Fact]
    public async Task TheToolExclusions_StillApplyToAMatchedAttention_ByTheCallsOwnStatement()
    {
        var (at, eventTime) = await SeedJudgedSpanAsync(MetadataServer);

        /* The attention row carries no statement, the call does. An azdata metadata call and a dbatools SMO call are left out. An azdata editor query is judged. */
        var cases = new (int Session, string Db, string App, string Text)[]
        {
            (301, "master", AttentionWindowSql.AzdataClientApp, "create table #SVer(ID int, Name sysname)"),
            (302, "db1", AttentionWindowSql.DbatoolsClientApp, "dbo.Smo: SELECT name FROM sys.schemas WHERE schema_id = @_msparam_0"),
            (303, "db1", AttentionWindowSql.AzdataClientApp, "SELECT * FROM dbo.Orders"),
        };
        foreach (var (session, db, app, text) in cases)
        {
            await RowAsync(MetadataServer, at, "sample", db, app, null, null, durationUs: 4_000, eventTime: eventTime, sessionId: session);
            await RowAsync(MetadataServer, at, "aborted", db, app, "fp-call", text, durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: session);
        }

        var (_, body) = await ReadToolAsync(MetadataServer);

        var excluded = body.GetProperty("excluded_from_alert");
        Assert.Equal(1, excluded.GetProperty("azdata_metadata_events").GetInt64());
        Assert.Equal(1, excluded.GetProperty("dbatools_metadata_events").GetInt64());
        Assert.Equal(1, excluded.GetProperty("matched_calls").GetInt64());
        Assert.Equal(0, excluded.GetProperty("below_duration_floor_events").GetInt64());
        Assert.Equal(55 / 3.0, excluded.GetProperty("count_judged").GetDouble(), 1);

        var calls = body.GetProperty("matched_calls").EnumerateArray().ToList();
        Assert.Single(calls);
        Assert.Equal(303, calls[0].GetProperty("session_id").GetInt32());
    }

    [Fact]
    public async Task AMatchedAttention_FromAnExcludedApp_IsStillLeftOut_AndNotCountedAsAMatchedCall()
    {
        var (at, eventTime) = await SeedJudgedSpanAsync(ExcludedAppServer);
        var intelliSense = AttentionWindowSql.SsmsIntelliSensePrefix + " - Transact-SQL " + AttentionWindowSql.SsmsIntelliSenseSuffix;

        await RowAsync(ExcludedAppServer, at, "sample", "db1", intelliSense, null, null, durationUs: 4_000, eventTime: eventTime, sessionId: 401);
        await RowAsync(ExcludedAppServer, at, "aborted", "db1", intelliSense, "fp-call", "SELECT name FROM sys.objects", durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 401);
        await RowAsync(ExcludedAppServer, at, "sample", "db1", "etl", null, null, durationUs: 4_000, eventTime: eventTime, sessionId: 402);
        await RowAsync(ExcludedAppServer, at, "aborted", "db1", "etl", "fp-call", "dbo.Slow: EXEC dbo.Slow", durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 402);

        var (reading, body) = await ReadToolAsync(ExcludedAppServer);

        /* Two sampled events: the IntelliSense one is left out by its app name and is not a matched call, the other is judged: 55 x 1/2. */
        Assert.Equal(1, reading.MatchedSamples);
        var excluded = body.GetProperty("excluded_from_alert");
        Assert.Equal(1, excluded.GetProperty("matched_calls").GetInt64());
        Assert.Equal(27.5, excluded.GetProperty("count_judged").GetDouble(), 1);
    }

    [Fact]
    public async Task AnUnderFloorAttention_WithNoAbortedCallRows_KeepsEveryNumberItHadBefore()
    {
        var (at, eventTime) = await SeedJudgedSpanAsync(NoCallServer);

        /* The #9030 shape: two 4 ms attentions, and a 30 s one, one at exactly 1 s and one with no duration. Session and event time are recorded, no call is. */
        await RowAsync(NoCallServer, at, "sample", "db1", "etl", "fp-ms", "SELECT 1", durationUs: 4_000, eventTime: eventTime, sessionId: 51);
        await RowAsync(NoCallServer, at, "sample", "db1", "web", "fp-ms", "SELECT 1", durationUs: 4_000, eventTime: eventTime, sessionId: 52);
        await RowAsync(NoCallServer, at, "sample", "db1", "etl", "fp-30s", "SELECT 2", durationUs: 30_000_000, eventTime: eventTime, sessionId: 53);
        await RowAsync(NoCallServer, at, "sample", "db1", "etl", "fp-1s", "SELECT 3", durationUs: 1_000_000, eventTime: eventTime, sessionId: 54);
        await RowAsync(NoCallServer, at, "sample", "db1", "etl", "fp-null", "SELECT 4", durationUs: null, eventTime: eventTime, sessionId: 55);

        var (_, body) = await ReadToolAsync(NoCallServer);

        Assert.Equal(55, body.GetProperty("count").GetInt64());
        Assert.Equal(5, body.GetProperty("sampled_events_in_range").GetInt64());
        var excluded = body.GetProperty("excluded_from_alert");
        Assert.Equal(2, excluded.GetProperty("below_duration_floor_events").GetInt64());
        Assert.Equal(0, excluded.GetProperty("matched_calls").GetInt64());
        Assert.Equal(0, excluded.GetProperty("azdata_metadata_events").GetInt64());
        Assert.Equal(0, excluded.GetProperty("dbatools_metadata_events").GetInt64());
        Assert.Equal(55 * 3 / 5.0, excluded.GetProperty("count_judged").GetDouble());
        Assert.Equal(0, body.GetProperty("matched_calls").GetArrayLength());

        var listed = body.GetProperty("sampled_events").EnumerateArray().ToList();
        Assert.Equal(2, listed.Count(e => e.GetProperty("below_duration_floor").GetBoolean()));
        Assert.DoesNotContain(listed, e => e.GetProperty("matched_call").GetBoolean());
    }

    [Fact]
    public async Task ASampleOverTheFloor_IsJudgedAsToday_WhetherOrNotACallMatches()
    {
        var (at, eventTime) = await SeedJudgedSpanAsync(OverFloorServer);

        /* Two attentions of 30 s and 1 s exactly, each with a matching call. Neither is under the floor, so neither is a matched sample. */
        await RowAsync(OverFloorServer, at, "sample", "db1", "etl", "fp-30s", "SELECT 2", durationUs: 30_000_000, eventTime: eventTime, sessionId: 61);
        await RowAsync(OverFloorServer, at, "sample", "db1", "etl", "fp-1s", "SELECT 3", durationUs: 1_000_000, eventTime: eventTime, sessionId: 62);
        await RowAsync(OverFloorServer, at, "aborted", "db1", "etl", "fp-call", "dbo.A: EXEC dbo.A", durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 61);
        await RowAsync(OverFloorServer, at, "aborted", "db1", "etl", "fp-call", "dbo.B: EXEC dbo.B", durationUs: 30_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 62);

        var now = DateTime.UtcNow;
        var (window, samples) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(OverFloorServer, now.AddHours(-1), now, 20);
        var (_, reading) = ClientTimeoutsRead.Classify(window, now);

        Assert.Equal(0, reading!.MatchedSamples);
        Assert.Equal(0, reading.ShortSamples);
        Assert.Empty(window.MatchedCalls);
        Assert.All(samples, s => Assert.False(s.MatchedCall));
        Assert.Equal(2, samples.Count);
    }

    [Fact]
    public async Task ACallThatRanUnderTheFloor_IsNotACommandTimeout_ButOneThatRanLongerIs()
    {
        var (at, eventTime) = await SeedJudgedSpanAsync(ShortCallServer);

        /* Three 4 ms attentions. Session 1 cancelled a call 50 ms into its run (a data layer cancelling before it closes a reader), session 2 a call
           that had run 5 s (a command timeout), session 3 a call that ran 0.95 s (a 1 s command timeout recorded a little short, under the 1 s attention floor but over the capture floor). Only the longer two are command timeouts. */
        for (var session = 1; session <= 3; session++)
        {
            await RowAsync(ShortCallServer, at, "sample", "db1", "app", null, null, durationUs: 4_000, eventTime: eventTime, sessionId: session);
        }

        await RowAsync(ShortCallServer, at, "aborted", "db1", "app", "fp-call", "dbo.A: EXEC dbo.A", durationUs: 50_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 1);
        await RowAsync(ShortCallServer, at, "aborted", "db1", "app", "fp-call", "dbo.B: EXEC dbo.B", durationUs: 5_000_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 2);
        await RowAsync(ShortCallServer, at, "aborted", "db1", "app", "fp-call", "dbo.C: EXEC dbo.C", durationUs: 950_000, eventTime: eventTime.AddMilliseconds(10), sessionId: 3);

        var window = await new LocalDataService(_duckDb).GetAttentionWindowAsync(ShortCallServer, Lookback);

        Assert.Equal(3, window.SampleGroups.Sum(g => g.Samples));
        Assert.Equal(2, window.SampleGroups.Sum(g => g.MatchedSamples));
        Assert.Equal(1, window.SampleGroups.Sum(g => g.ShortSamples));

        var now = DateTime.UtcNow;
        var (range, samples) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(ShortCallServer, now.AddHours(-1), now, 20);
        Assert.Equal(2, range.MatchedCalls.Count);
        Assert.DoesNotContain(range.MatchedCalls, c => c.ElapsedUs == 50_000);
        Assert.Equal(2, samples.Count(s => s.MatchedCall));
    }

    /// <summary>The range read and the shared payload, as get_client_timeouts builds them.</summary>
    private async Task<(AttentionReading Reading, JsonElement Body)> ReadToolAsync(int serverId)
    {
        var now = DateTime.UtcNow;
        var (window, samples) = await new LocalDataService(_duckDb).GetAttentionRangeAsync(serverId, now.AddHours(-1), now, 20);
        var (kind, reading) = ClientTimeoutsRead.Classify(window, now);
        Assert.Equal(ClientTimeoutsKind.Data, kind);
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(ClientTimeoutsRead.Payload(reading!, window, samples, 20, 2000)));
        return (reading!, doc.RootElement.Clone());
    }

    /// <summary>Seeds a server whose collector ran 12 times a minute apart, with 5 attentions counted by every run (55 in the span), and returns the
    /// collection time the sampled rows are written at plus the event time the attention samples carry (a second before it).</summary>
    private async Task<(DateTime At, DateTime EventTime)> SeedJudgedSpanAsync(int serverId)
    {
        var now = DateTime.UtcNow;
        var runs = new List<DateTime>();
        for (var i = 0; i < 12; i++)
        {
            var run = now.AddSeconds(-30).AddMinutes(-i);
            runs.Add(run);
            await LogAsync(serverId, run);
            await RowAsync(serverId, run.AddSeconds(2), "count", null, null, null, null, count: 5);
        }

        var at = runs[1].AddSeconds(2);
        return (at, at.AddSeconds(-1));
    }

    private async Task LogAsync(int serverId, DateTime time)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
VALUES ($1, $2, 'test-server', $3, $4, 5, 'SUCCESS')";
        command.Parameters.Add(new DuckDBParameter { Value = _id++ });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = AttentionWindowSql.CollectorName });
        command.Parameters.Add(new DuckDBParameter { Value = time });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task RowAsync(
        int serverId, DateTime time, string kind, string? db, string? app, string? fingerprint, string? text,
        long count = 0, long? durationUs = null, DateTime? eventTime = null, int? sessionId = null, string? host = null)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO attention_events
    (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name, client_app_name, statement_fingerprint, statement_text, interrupt_duration_us, event_time, session_id, client_hostname)
VALUES ($1, $2, $3, 'test-server', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)";
        command.Parameters.Add(new DuckDBParameter { Value = _id++ });
        command.Parameters.Add(new DuckDBParameter { Value = time });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = kind });
        command.Parameters.Add(new DuckDBParameter { Value = count });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)db ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)app ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)fingerprint ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)text ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)durationUs ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)eventTime ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)sessionId ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)host ?? DBNull.Value });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
